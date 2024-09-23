import 'reflect-metadata';
import { DataSource } from 'typeorm';
import { Cinema } from './entity/Cinema';
import Koa from 'koa';
import Router from 'koa-router';
import bodyParser from 'koa-bodyparser';
import { createClient } from 'redis';
import cluster from 'cluster';
import os from 'os';

const numCPUs = os.cpus().length;

// Create a new DataSource instance
const AppDataSource = new DataSource({
    type: "postgres",
    host: "localhost",
    port: 5432,
    username: "postgres",
    password: "mysecretpassword",
    database: "ticket",
    synchronize: true,  // Automatically create database schema
    logging: false,
    entities: [Cinema],
});

async function startServer() {
    const app = new Koa();
    const router = new Router();
    const redis = createClient();
    redis.on('error', (err: any) => console.log('Redis Client Error', err));
    await redis.connect();

    // Initialize the DataSource and wait for it to connect
    await AppDataSource.initialize();

    // Create a cinema with N seats
    router.post('/cinemas', async (ctx) => {
        const { seats } = ctx.request.body as { seats: number};
        const cinema: Cinema = { seats: new Array(seats).fill(false) }; // false means seat is free

        const cinemaRepository = AppDataSource.getRepository(Cinema);
        const savedObj = await cinemaRepository.save(cinema);

        ctx.status = 201;
        ctx.body = { id: savedObj.id };
    });

    // Purchase a specific seat number in cinema C
    router.post('/cinemas/:id/purchase', async (ctx) => {
        const cinemaId = ctx.params.id;
        const { seatNumber } = ctx.request.body as any;

        const cinemaRepository = AppDataSource.getRepository(Cinema);
        const cinema = await cinemaRepository.findOneBy({ id: cinemaId });

        if (!cinema) {
            ctx.status = 404;
            ctx.body = { error: 'Cinema not found' };
            return;
        }

        if (seatNumber < 1 || seatNumber > cinema.seats.length) {
            ctx.status = 400;
            ctx.body = { error: 'Invalid seat number' };
            return;
        }

        const seatIndex = seatNumber - 1;

        // Using Redis to implement a lock
        const lockKey = `lock:${cinemaId}:${seatNumber}`;
        const lockAcquired = await redis.set(lockKey, 'locked', { 'EX': 5 }); // 5 seconds expiration

        if (!lockAcquired) {
            ctx.status = 429; // Too many requests
            ctx.body = { error: 'Seat is currently being purchased by another request' };
            return;
        }

        try {
            if (cinema.seats[seatIndex]) {
                ctx.status = 400;
                ctx.body = { error: 'Seat already purchased' };
                return;
            }

            cinema.seats[seatIndex] = true;
            await cinemaRepository.save(cinema);
            ctx.status = 200;
            ctx.body = { seat: seatNumber };
        } finally {
            // Release the lock
            await redis.del(lockKey);
        }
    });

    // Purchase the first two free consecutive seats in cinema C
    router.post('/cinemas/:id/purchase/consecutive', async (ctx) => {
        const cinemaId = ctx.params.id;
        
        const cinemaRepository = AppDataSource.getRepository(Cinema);
        const cinema = await cinemaRepository.findOneBy({ id: cinemaId });

        if (!cinema) {
            ctx.status = 404;
            ctx.body = { error: 'Cinema not found' };
            return;
        }

        const seats = cinema.seats;
        let foundSeats: number[] = [];

        for (let i = 0; i < seats.length - 1; i++) {
            if (!seats[i] && !seats[i + 1]) {
                foundSeats = [i + 1, i + 2]; // 1-based indexing
                break;
            }
        }

        if (foundSeats.length === 0) {
            ctx.status = 400;
            ctx.body = { error: 'No two consecutive seats available' };
            return;
        }

        // Lock mechanism for consecutive seat purchase
        const lockKey = `lock:${cinemaId}:consecutive`;
        const lockAcquired = await redis.set(lockKey, 'locked', { 'EX': 5 }); // 5 seconds expiration

        if (!lockAcquired) {
            ctx.status = 429; // Too many requests
            ctx.body = { error: 'Seats are currently being purchased by another request' };
            return;
        }

        try {
            foundSeats.forEach(seat => {
                if (seats[seat - 1]) {
                    ctx.status = 400;
                    ctx.body = { error: 'Seat already purchased' };
                    return;
                }
                seats[seat - 1] = true; // Mark as purchased
            });

            await cinemaRepository.save(cinema);
            ctx.status = 200;
            ctx.body = { seats: foundSeats };
        } finally {
            // Release the lock
            await redis.del(lockKey);
        }
    });

    // Apply the routes to the Koa app
    app.use(bodyParser());
    app.use(router.routes()).use(router.allowedMethods());

    // Start the server
    const PORT = process.env.PORT || 3000;
    app.listen(PORT, () => {
        console.log(`Worker ${process.pid} is running on http://localhost:${PORT}`);
    });
};

// If we're in the master process, fork workers
if (cluster.isPrimary) {
    console.log(`Master ${process.pid} is running`);

    // Fork workers equal to the number of CPU cores
    for (let i = 0; i < numCPUs; i++) {
        cluster.fork();
    }

    cluster.on('exit', (worker, code, signal) => {
        console.log(`Worker ${worker.process.pid} died`);
    });
} else {
    // Workers can share any TCP connection
    startServer();
}
