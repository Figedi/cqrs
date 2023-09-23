export class ApplicationError extends Error {}

export class NoHandlerFoundError extends Error {}
export class EventByIdNotFoundError extends Error {}
export class EventIdMissingError extends Error {}
export class TimeoutExceededError extends Error {}
export class RetriesExceededError extends Error {}
export class StreamEndedError extends Error {}
export class ConfigError extends Error {}
export class UnknownStreamIdError extends Error {}
export class TxTimeoutError extends Error {
  constructor(
    public message: string,
    public code: string,
  ) {
    super(message);
  }
}
