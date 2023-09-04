import { createEvent } from "../common.js";
import type { StringEither } from "../types.js";

export interface ITimePassedPayload {
  now: Date;
}

export class MinutePassed extends createEvent<ITimePassedPayload, StringEither>() {}
export class HourPassed extends createEvent<ITimePassedPayload, StringEither>() {}
export class DayPassed extends createEvent<ITimePassedPayload, StringEither>() {}
export class WeekPassed extends createEvent<ITimePassedPayload, StringEither>() {}

export interface ISagaTriggeredCommandPayload {
  sagaName: string;
  outgoingEventId?: string;
  outgoingEventName: string;
}

export class SagaTriggeredCommandEvent extends createEvent<ISagaTriggeredCommandPayload, StringEither>() {}
