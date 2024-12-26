import { EOL } from 'os'
import { ulid } from 'ulid'
import {
  DistributedTransactionEvents,
  DistributedTransactionType,
  LocalWorkflow,
  TransactionHandlerType,
  TransactionState,
} from '../../orchestrator'
import { Context } from '../../utils/types'
import { MedusaWorkflow } from '../medusa-workflow'
import { resolveValue } from '../utils/composer/helpers/resolve-value'
import {
  ExportedWorkflow,
  FlowCancelOptions,
  FlowRegisterStepFailureOptions,
  FlowRegisterStepSuccessOptions,
  FlowRunOptions,
  MainExportedWorkflow,
  WorkflowResult,
} from './type'

function createContextualWorkflowRunner<
  TData = unknown,
  TResult = unknown,
  TDataOverride = undefined,
  TResultOverride = undefined,
>({
  workflowId,
  defaultResult,
  options,
}: {
  workflowId: string
  defaultResult?: string | Symbol
  options?: {
    wrappedInput?: boolean
    sourcePath?: string
  }
}): Omit<LocalWorkflow, 'run' | 'registerStepSuccess' | 'registerStepFailure' | 'cancel'> &
  ExportedWorkflow<TData, TResult, TDataOverride, TResultOverride> {
  const flow = new LocalWorkflow(workflowId)

  const originalRun = flow.run.bind(flow)
  const originalRegisterStepSuccess = flow.registerStepSuccess.bind(flow)
  const originalRegisterStepFailure = flow.registerStepFailure.bind(flow)
  const originalCancel = flow.cancel.bind(flow)

  const originalExecution = async (
    method,
    { throwOnError, logOnError = false, resultFrom, isCancel = false },
    transactionOrIdOrIdempotencyKey: DistributedTransactionType | string,
    input: unknown,
    context: Context,
    events: DistributedTransactionEvents | undefined = {},
  ) => {
    const { eventGroupId, parentStepIdempotencyKey } = context

    attachOnFinishReleaseEvents(events, { logOnError })

    const flowMetadata = {
      eventGroupId,
      parentStepIdempotencyKey,
      sourcePath: options?.sourcePath,
    }

    const args = [transactionOrIdOrIdempotencyKey, input, context, events, flowMetadata]
    const transaction = (await method.apply(method, args)) as DistributedTransactionType

    let errors = transaction.getErrors(TransactionHandlerType.INVOKE)

    const failedStatus = [TransactionState.FAILED, TransactionState.REVERTED]
    const isCancelled = isCancel && transaction.getState() === TransactionState.REVERTED

    const isRegisterStepFailure =
      method === originalRegisterStepFailure && transaction.getState() === TransactionState.REVERTED

    let thrownError = null

    if (failedStatus.includes(transaction.getState()) && !isCancelled && !isRegisterStepFailure) {
      const firstError = errors?.[0]?.error ?? new Error('Unknown error')

      thrownError = firstError

      if (throwOnError) {
        throw firstError
      }
    }

    let result
    if (options?.wrappedInput) {
      result = resolveValue(resultFrom, transaction.getContext())
      if (result instanceof Promise) {
        result = await result.catch(e => {
          thrownError = e

          if (throwOnError) {
            throw e
          }

          errors ??= []
          errors.push(e)
        })
      }
    } else {
      result = transaction.getContext().invoke?.[resultFrom]
    }

    return {
      errors,
      transaction,
      result,
      thrownError,
    }
  }

  const newRun = async ({
    input,
    context: outerContext,
    throwOnError,
    logOnError,
    resultFrom,
    events,
  }: FlowRunOptions = {}) => {
    resultFrom ??= defaultResult
    throwOnError ??= true
    logOnError ??= false

    const context = {
      ...outerContext,
    }

    context.transactionId ??= ulid()
    context.eventGroupId ??= ulid()

    return await originalExecution(
      originalRun,
      {
        throwOnError,
        resultFrom,
        logOnError,
      },
      context.transactionId,
      input,
      context,
      events,
    )
  }
  flow.run = newRun as any

  const newRegisterStepSuccess = async (
    {
      response,
      idempotencyKey,
      context: outerContext,
      throwOnError,
      logOnError,
      resultFrom,
      events,
    }: FlowRegisterStepSuccessOptions = {
      idempotencyKey: '',
    },
  ) => {
    idempotencyKey ??= ''
    resultFrom ??= defaultResult
    throwOnError ??= true
    logOnError ??= false

    const [, transactionId] = idempotencyKey.split(':')
    const context = {
      ...outerContext,
      transactionId,
    }

    context.eventGroupId ??= ulid()

    return await originalExecution(
      originalRegisterStepSuccess,
      {
        throwOnError,
        resultFrom,
        logOnError,
      },
      idempotencyKey,
      response,
      context,
      events,
    )
  }
  flow.registerStepSuccess = newRegisterStepSuccess as any

  const newRegisterStepFailure = async (
    {
      response,
      idempotencyKey,
      context: outerContext,
      throwOnError,
      logOnError,
      resultFrom,
      events,
    }: FlowRegisterStepFailureOptions = {
      idempotencyKey: '',
    },
  ) => {
    idempotencyKey ??= ''
    resultFrom ??= defaultResult
    throwOnError ??= true
    logOnError ??= false

    const [, transactionId] = idempotencyKey.split(':')
    const context = {
      ...outerContext,
      transactionId,
    }

    context.eventGroupId ??= ulid()

    return await originalExecution(
      originalRegisterStepFailure,
      {
        throwOnError,
        resultFrom,
        logOnError,
      },
      idempotencyKey,
      response,
      context,
      events,
    )
  }
  flow.registerStepFailure = newRegisterStepFailure as any

  const newCancel = async ({
    transaction,
    transactionId,
    context: outerContext,
    throwOnError,
    logOnError,
    events,
  }: FlowCancelOptions = {}) => {
    throwOnError ??= true
    logOnError ??= false

    const context = {
      ...outerContext,
      transactionId,
    }

    context.eventGroupId ??= ulid()

    return await originalExecution(
      originalCancel,
      {
        throwOnError,
        resultFrom: undefined,
        isCancel: true,
        logOnError,
      },
      transaction ?? transactionId!,
      undefined,
      context,
      events,
    )
  }
  flow.cancel = newCancel as any

  return flow as unknown as LocalWorkflow &
    ExportedWorkflow<TData, TResult, TDataOverride, TResultOverride>
}

export const exportWorkflow = <TData = unknown, TResult = unknown>(
  workflowId: string,
  defaultResult?: string | Symbol,
  options?: {
    wrappedInput?: boolean
    sourcePath?: string
  },
): MainExportedWorkflow<TData, TResult> => {
  function exportedWorkflow<TDataOverride = undefined, TResultOverride = undefined>(): Omit<
    LocalWorkflow,
    'run' | 'registerStepSuccess' | 'registerStepFailure' | 'cancel'
  > &
    ExportedWorkflow<TData, TResult, TDataOverride, TResultOverride> {
    return createContextualWorkflowRunner<TData, TResult, TDataOverride, TResultOverride>({
      workflowId,
      defaultResult,
      options,
    })
  }

  const buildRunnerFn = <
    TAction extends 'run' | 'registerStepSuccess' | 'registerStepFailure' | 'cancel',
    TDataOverride,
    TResultOverride,
  >(
    action: 'run' | 'registerStepSuccess' | 'registerStepFailure' | 'cancel',
  ) => {
    const contextualRunner = createContextualWorkflowRunner<
      TData,
      TResult,
      TDataOverride,
      TResultOverride
    >({
      workflowId,
      defaultResult,
      options,
    })

    return contextualRunner[action] as ExportedWorkflow<
      TData,
      TResult,
      TDataOverride,
      TResultOverride
    >[TAction]
  }

  exportedWorkflow.run = async <TDataOverride = undefined, TResultOverride = undefined>(
    args?: FlowRunOptions<TDataOverride extends undefined ? TData : TDataOverride>,
  ): Promise<WorkflowResult<TResultOverride extends undefined ? TResult : TResultOverride>> => {
    const inputArgs = { ...args } as FlowRunOptions<
      TDataOverride extends undefined ? TData : TDataOverride
    >

    return await buildRunnerFn<'run', TDataOverride, TResultOverride>('run')(inputArgs)
  }

  exportedWorkflow.registerStepSuccess = async <
    TDataOverride = undefined,
    TResultOverride = undefined,
  >(
    args?: FlowRegisterStepSuccessOptions<TDataOverride extends undefined ? TData : TDataOverride>,
  ): Promise<WorkflowResult<TResultOverride extends undefined ? TResult : TResultOverride>> => {
    const inputArgs = { ...args } as FlowRegisterStepSuccessOptions<
      TDataOverride extends undefined ? TData : TDataOverride
    >

    return await buildRunnerFn<'registerStepSuccess', TDataOverride, TResultOverride>(
      'registerStepSuccess',
    )(inputArgs)
  }

  exportedWorkflow.registerStepFailure = async <
    TDataOverride = undefined,
    TResultOverride = undefined,
  >(
    args?: FlowRegisterStepFailureOptions<TDataOverride extends undefined ? TData : TDataOverride>,
  ): Promise<WorkflowResult<TResultOverride extends undefined ? TResult : TResultOverride>> => {
    const inputArgs = { ...args } as FlowRegisterStepFailureOptions<
      TDataOverride extends undefined ? TData : TDataOverride
    >

    return await buildRunnerFn<'registerStepFailure', TDataOverride, TResultOverride>(
      'registerStepFailure',
    )(inputArgs)
  }

  exportedWorkflow.cancel = async (args?: FlowCancelOptions): Promise<WorkflowResult> => {
    const inputArgs = { ...args } as FlowCancelOptions

    return await buildRunnerFn<'cancel', unknown, unknown>('cancel')(inputArgs)
  }

  MedusaWorkflow.registerWorkflow(workflowId, exportedWorkflow)
  return exportedWorkflow as MainExportedWorkflow<TData, TResult>
}

function attachOnFinishReleaseEvents(
  events: DistributedTransactionEvents = {},
  {
    logOnError,
  }: {
    logOnError?: boolean
  } = {},
) {
  const onFinish = events.onFinish

  const wrappedOnFinish = async (args: {
    transaction: DistributedTransactionType
    result?: unknown
    errors?: unknown[]
  }) => {
    const { transaction } = args

    if (logOnError) {
      const workflowName = transaction.getFlow().modelId
      transaction
        .getErrors()
        .forEach(err =>
          console.error(
            `${workflowName}:${err?.action}:${err?.handlerType} - ${err?.error?.message}${EOL}${err?.error?.stack}`,
          ),
        )
    }

    await onFinish?.(args)
  }

  events.onFinish = wrappedOnFinish
}
