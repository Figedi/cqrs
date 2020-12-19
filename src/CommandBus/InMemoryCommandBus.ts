import { isLeft, right } from "fp-ts/lib/Either";
import { v4 as uuid } from "uuid";

import { AnyEither, ExecuteOpts, ICommand, ICommandBus, StringEither, VoidEither } from "../types";
import { BaseCommandBus } from "./BaseCommandBus";

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

  public async replay<T, TRes extends StringEither, TCommandRes extends VoidEither>(
    command: ICommand<T, TCommandRes>,
  ): Promise<TRes> {
    return this.execute(command);
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
