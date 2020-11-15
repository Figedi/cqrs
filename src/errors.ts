import VError from "verror";

export class ApplicationError extends VError {}

export class NoHandlerFoundError extends VError {}
export class EventIdMissingError extends VError {}
export class TimeoutExceededError extends VError {}
export class RetriesExceededError extends VError {}
export class StreamEndedError extends VError {}
export class ConfigError extends VError {}
export class UnknownStreamIdError extends VError {}
