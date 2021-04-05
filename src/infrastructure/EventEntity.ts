import { Column, Entity, OneToMany, PrimaryColumn } from "typeorm";

import { IMeta, ISerializedEvent } from "../types";
import { ScheduledEventEntity } from "./ScheduledEventEntity";

@Entity("events")
export class EventEntity {
  @PrimaryColumn({ name: "event_id", type: "text" })
  eventId!: string;

  @Column({ name: "event_name", type: "text" })
  eventName!: string;

  @Column({ name: "stream_id", type: "text" })
  streamId!: string;

  @Column({ name: "event", type: "jsonb" })
  event!: ISerializedEvent<any>;

  @Column({ name: "timestamp", type: "timestamptz" })
  timestamp!: Date;

  @Column({ name: "status", type: "text" })
  status!: "CREATED" | "PROCESSING" | "FAILED" | "PROCESSED";

  @Column({ name: "type", type: "text" })
  type!: "COMMAND" | "QUERY" | "EVENT";

  @OneToMany(() => ScheduledEventEntity, schedule => schedule.event)
  schedules?: ScheduledEventEntity[];

  @Column({ name: "meta", type: "jsonb", nullable: true })
  meta?: IMeta;
}
