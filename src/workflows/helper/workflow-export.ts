import { EOL } from 'os'
import { ulid } from 'ulid'
import {
  DistributedTransaction,
  DistributedTransactionEvents,
  LocalWorkflow,
  TransactionHandlerType,
  TransactionState,
  TransactionStepError,
} from '../../orchestrator'
import { OrchestrationUtils } from '../../utils'
import { MedusaWorkflow } from '../medusa-workflow'
import { resolveValue } from '../utils/composer'

export type FlowRunOptions<TData = unknown> = {
  input?: TData
  resultFrom?: string | string[] | Symbol
  throwOnError?: boolean
  events?: DistributedTransactionEvents
}

export type FlowRegisterStepSuccessOptions<TData = unknown> = {
  idempotencyKey: string
  response?: TData
  resultFrom?: string | string[] | Symbol
  throwOnError?: boolean
  events?: DistributedTransactionEvents
}

export type FlowRegisterStepFailureOptions<TData = unknown> = {
  idempotencyKey: string
  response?: TData
  resultFrom?: string | string[] | Symbol
  throwOnError?: boolean
  events?: DistributedTransactionEvents
}

export type WorkflowResult<TResult = unknown> = {
  errors: TransactionStepError[]
  transaction: DistributedTransaction
  result: TResult
}

export type ExportedWorkflow<
  TData = unknown,
  TResult = unknown,
  TDataOverride = undefined,
  TResultOverride = undefined,
> = {
  run: (
    args?: FlowRunOptions<TDataOverride extends undefined ? TData : TDataOverride>,
  ) => Promise<WorkflowResult<TResultOverride extends undefined ? TResult : TResultOverride>>
  registerStepSuccess: (
    args?: FlowRegisterStepSuccessOptions<TDataOverride extends undefined ? TData : TDataOverride>,
  ) => Promise<WorkflowResult<TResultOverride extends undefined ? TResult : TResultOverride>>
  registerStepFailure: (
    args?: FlowRegisterStepFailureOptions<TDataOverride extends undefined ? TData : TDataOverride>,
  ) => Promise<WorkflowResult<TResultOverride extends undefined ? TResult : TResultOverride>>
}

export const exportWorkflow = <TData = unknown, TResult = unknown>(
  workflowId: string,
  defaultResult?: string | Symbol,
  dataPreparation?: (data: TData) => Promise<unknown>,
  options?: {
    wrappedInput?: boolean
  },
) => {
  function exportedWorkflow<TDataOverride = undefined, TResultOverride = undefined>(): Omit<
    LocalWorkflow,
    'run' | 'registerStepSuccess' | 'registerStepFailure'
  > &
    ExportedWorkflow<TData, TResult, TDataOverride, TResultOverride> {
    const flow = new LocalWorkflow(workflowId)

    const originalRun = flow.run.bind(flow)
    const originalRegisterStepSuccess = flow.registerStepSuccess.bind(flow)
    const originalRegisterStepFailure = flow.registerStepFailure.bind(flow)

    const originalExecution = async (method, { throwOnError, resultFrom }, ...args) => {
      const transaction = await method.apply(method, args)

      const errors = transaction.getErrors(TransactionHandlerType.INVOKE)

      const failedStatus = [TransactionState.FAILED, TransactionState.REVERTED]
      if (failedStatus.includes(transaction.getState()) && throwOnError) {
        const errorMessage = errors
          ?.map(err => `${err.error?.message}${EOL}${err.error?.stack}`)
          ?.join(`${EOL}`)
        throw new Error(errorMessage)
      }

      let result: any = undefined

      const resFrom =
        resultFrom?.__type === OrchestrationUtils.SymbolWorkflowStep
          ? resultFrom.__step__
          : resultFrom

      if (resFrom) {
        if (Array.isArray(resFrom)) {
          result = resFrom.map(from => {
            const res = transaction.getContext().invoke?.[from]
            return res?.__type === OrchestrationUtils.SymbolWorkflowWorkflowData ? res.output : res
          })
        } else {
          const res = transaction.getContext().invoke?.[resFrom]
          result = res?.__type === OrchestrationUtils.SymbolWorkflowWorkflowData ? res.output : res
        }

        const ret = result || resFrom
        result = options?.wrappedInput ? await resolveValue(ret, transaction.getContext()) : ret
      }

      return {
        errors,
        transaction,
        result,
      }
    }

    const newRun = async (
      { input, throwOnError, resultFrom, events }: FlowRunOptions = {
        throwOnError: true,
        resultFrom: defaultResult,
      },
    ) => {
      resultFrom ??= defaultResult
      throwOnError ??= true

      if (typeof dataPreparation === 'function') {
        try {
          const copyInput = input ? JSON.parse(JSON.stringify(input)) : input
          input = await dataPreparation(copyInput as TData)
        } catch (err) {
          if (throwOnError) {
            throw new Error(`Data preparation failed: ${err.message}${EOL}${err.stack}`)
          }
          return {
            errors: [err],
          }
        }
      }

      return await originalExecution(
        originalRun,
        { throwOnError, resultFrom },
        /* context?.transactionId ?? */ ulid(),
        input,
        events,
      )
    }
    flow.run = newRun as any

    const newRegisterStepSuccess = async (
      {
        response,
        idempotencyKey,
        throwOnError,
        resultFrom,
        events,
      }: FlowRegisterStepSuccessOptions = {
        idempotencyKey: '',
        throwOnError: true,
        resultFrom: defaultResult,
      },
    ) => {
      resultFrom ??= defaultResult
      throwOnError ??= true

      return await originalExecution(
        originalRegisterStepSuccess,
        { throwOnError, resultFrom },
        idempotencyKey,
        response,
        events,
      )
    }
    flow.registerStepSuccess = newRegisterStepSuccess as any

    const newRegisterStepFailure = async (
      {
        response,
        idempotencyKey,
        throwOnError,
        resultFrom,
        events,
      }: FlowRegisterStepFailureOptions = {
        idempotencyKey: '',
        throwOnError: true,
        resultFrom: defaultResult,
      },
    ) => {
      resultFrom ??= defaultResult
      throwOnError ??= true

      return await originalExecution(
        originalRegisterStepFailure,
        { throwOnError, resultFrom },
        idempotencyKey,
        response,
        events,
      )
    }
    flow.registerStepFailure = newRegisterStepFailure as any

    return flow as unknown as LocalWorkflow &
      ExportedWorkflow<TData, TResult, TDataOverride, TResultOverride>
  }

  MedusaWorkflow.registerWorkflow(workflowId, exportedWorkflow)
  return exportedWorkflow
}
