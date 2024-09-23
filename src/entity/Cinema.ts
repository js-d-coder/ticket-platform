import 'reflect-metadata';
import { Entity, PrimaryGeneratedColumn, Column } from 'typeorm';

@Entity()
export class Cinema {
    @PrimaryGeneratedColumn()
    id?: string;

    @Column('boolean', { array: true })
    seats!: boolean[];
}
