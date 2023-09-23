import type { AnyEither, ExecuteOpts, ICommand, ICommandBus, StringEither, VoidEither } from "../types.js";
import { isLeft, right } from "fp-ts/lib/Either.js";

import { BaseCommandBus } from "./BaseCommandBus.js";
import { v4 as uuid } from "uuid";

export class InMemoryCommandBus extends BaseCommandBus implements ICommandBus {
  public async drain(): Promise<void> {
    // do nothing
  }

  public async execute<T, TRes extends StringEither, TCommandRes extends VoidEither>(
    command: ICommand<T, TCommandRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes> {
    const eventId = command.meta?.eventId || opts?.eventId || uuid();
    const streamId = command.meta?.streamId || opts?.streamId || eventId;
    // eslint-disable-next-line no-param-reassign
    command.meta = { ...command.meta, eventId };

    this.in$.next({ command, scope: {} as any });

    return right(streamId) as TRes;
  }

  public async replayAllFailed() {
    throw new Error("Not supported");
  }

  public async replay<T, TRes extends StringEither, TCommandRes extends VoidEither>(
    command: ICommand<T, TCommandRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes> {
    return this.execute(command, opts);
  }

  public async executeSync<T, TRes extends AnyEither, TCommandRes extends AnyEither>(
    command: ICommand<T, TCommandRes>,
    opts?: ExecuteOpts,
  ): Promise<TRes> {
    const executeResult = await this.execute(command, opts);
    if (isLeft(executeResult)) {
      return executeResult as TRes;
    }
    return this.waitForCommandResult<TRes>(command.meta.className, executeResult.right, opts?.timeout || 0);
  }
}
