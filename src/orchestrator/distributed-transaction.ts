import { TransactionFlow, TransactionHandlerType, TransactionState } from './types'

/**
 * @typedef {Object} TransactionMetadata
 * @property {string} producer - The id of the producer that created the transaction (transactionModelId).
 * @property {string} reply_to_topic - The topic to reply to for the transaction.
 * @property {string} idempotency_key - The idempotency key of the transaction.
 * @property {string} action - The action of the transaction.
 * @property {TransactionHandlerType} action_type - The type of the transaction.
 * @property {number} attempt - The number of attempts for the transaction.
 * @property {number} timestamp - The timestamp of the transaction.
 */
export type TransactionMetadata = {
  producer: string
  reply_to_topic: string
  idempotency_key: string
  action: string
  action_type: TransactionHandlerType
  attempt: number
  timestamp: number
}

export class TransactionPayload {
  /**
   * @param metadata - The metadata of the transaction.
   * @param data - The payload data of the transaction and the response of the previous step if forwardResponse is true.
   */
  constructor(
    public metadata: TransactionMetadata,
    public data: Record<string, unknown> & {
      _response: Record<string, unknown>
    },
  ) {}
}

export type TransactionStepHandler = (
  actionId: string,
  handlerType: TransactionHandlerType,
  payload: TransactionPayload,
) => Promise<unknown>

export class DistributedTransaction {
  public transactionId: string
  public modelId: string
  public errors: {
    action: string
    handlerType: TransactionHandlerType
    error: Error | null
  }[] = []

  constructor(
    private flow: TransactionFlow,
    public handler: (
      actionId: string,
      handlerType: TransactionHandlerType,
      payload: TransactionPayload,
    ) => Promise<unknown>,
    public payload?: any,
  ) {
    this.transactionId = flow.transactionId
    this.modelId = flow.transactionModelId
  }

  public getState(): TransactionState {
    return this.getFlow().state
  }

  public get isPartiallyCompleted(): boolean {
    return !!this.getFlow().hasFailedSteps || !!this.getFlow().hasSkippedSteps
  }

  public getFlow() {
    return this.flow
  }

  public hasFinished(): boolean {
    return [TransactionState.DONE, TransactionState.REVERTED, TransactionState.FAILED].includes(
      this.getState(),
    )
  }

  public addError(action: string, handlerType: TransactionHandlerType, error: Error | null) {
    this.errors.push({
      action,
      handlerType,
      error,
    })
  }
}
