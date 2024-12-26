import {
  DistributedTransaction,
  TransactionMetadata,
  WorkflowStepHandler,
} from '../../orchestrator'
import { mergeData } from './merge-data'

export type WorkflowStepMiddlewareReturn = {
  alias?: string
  value: any
}

export type WorkflowStepMiddlewareInput = {
  from: string
  alias?: string
}

interface PipelineInput {
  /**
   * The alias of the input data to store in
   */
  inputAlias?: string
  /**
   * Descriptors to get the data from
   */
  invoke?: WorkflowStepMiddlewareInput | WorkflowStepMiddlewareInput[]
  compensate?: WorkflowStepMiddlewareInput | WorkflowStepMiddlewareInput[]
  onComplete?: (args: WorkflowOnCompleteArguments) => Promise<void>
  /**
   * Apply the data merging
   */
  merge?: boolean
  /**
   * Store the merged data in a new key, if this is present no need to set merge: true
   */
  mergeAlias?: string
  /**
   * Store the merged data from the chosen aliases, if this is present no need to set merge: true
   */
  mergeFrom?: string[]
}

export type WorkflowArguments<T = any> = {
  payload: unknown
  data: T
  metadata: TransactionMetadata
}

export type WorkflowOnCompleteArguments<T = any> = {
  payload: unknown
  data: T
  metadata: TransactionMetadata
  transaction: DistributedTransaction
}

export type PipelineHandler<T extends any = undefined> = (
  args: WorkflowArguments,
) => Promise<
  T extends undefined ? WorkflowStepMiddlewareReturn | WorkflowStepMiddlewareReturn[] : T
>

export function pipe<T>(
  input: PipelineInput,
  ...functions: [...PipelineHandler[], PipelineHandler<T>]
): WorkflowStepHandler {
  // Apply the aggregator just before the last handler
  if ((input.merge || input.mergeAlias || input.mergeFrom) && functions.length) {
    const handler = functions.pop()!
    functions.push(mergeData(input.mergeFrom, input.mergeAlias), handler)
  }

  return async ({ payload, invoke, compensate, metadata, transaction }) => {
    let data = {}

    const original = {
      invoke: invoke ?? {},
      compensate: compensate ?? {},
    }

    if (input.inputAlias) {
      Object.assign(original.invoke, { [input.inputAlias]: payload })
    }

    const dataKeys = ['invoke', 'compensate']
    for (const key of dataKeys) {
      if (!input[key]) {
        continue
      }

      if (!Array.isArray(input[key])) {
        input[key] = [input[key]]
      }

      for (const action of input[key]) {
        if (action.alias) {
          data[action.alias] = original[key][action.from]
        } else {
          data[action.from] = original[key][action.from]
        }
      }
    }

    let finalResult
    for (const fn of functions) {
      let result = await fn({
        payload,
        data,
        metadata,
      })

      if (Array.isArray(result)) {
        for (const action of result) {
          if (action?.alias) {
            data[action.alias] = action.value
          }
        }
      } else if (result && 'alias' in (result as WorkflowStepMiddlewareReturn)) {
        if ((result as WorkflowStepMiddlewareReturn).alias) {
          data[(result as WorkflowStepMiddlewareReturn).alias!] = (
            result as WorkflowStepMiddlewareReturn
          ).value
        } else {
          data = (result as WorkflowStepMiddlewareReturn).value
        }
      }

      finalResult = result
    }

    if (typeof input.onComplete === 'function') {
      const dataCopy = JSON.parse(JSON.stringify(data))
      await input.onComplete({
        payload,
        data: dataCopy,
        metadata,
        transaction,
      })
    }

    return finalResult
  }
}
