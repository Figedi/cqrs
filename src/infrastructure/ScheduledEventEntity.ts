import { Column, Entity, PrimaryColumn } from "typeorm";
import { ISerializedEvent } from "../types";

@Entity("scheduled_events")
export class ScheduledEventEntity {
  @PrimaryColumn({ name: "scheduled_event_id", type: "text" })
  scheduledEventId!: string;

  @Column({ name: "execute_at", type: "timestamptz" })
  executeAt!: Date;

  @Column({ name: "event", type: "jsonb", nullable: false })
  event!: ISerializedEvent<any>;

  @Column({ name: "status", type: "text" })
  status!: "CREATED" | "FAILED" | "ABORTED" | "PROCESSED";
}
