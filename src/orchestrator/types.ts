import { TransactionStep } from './transaction-step'

export enum TransactionStepStatus {
  IDLE = 'idle',
  OK = 'ok',
  WAITING = 'waiting_response',
  TEMPORARY_FAILURE = 'temp_failure',
  PERMANENT_FAILURE = 'permanent_failure',
}

export enum TransactionState {
  NOT_STARTED = 'not_started',
  INVOKING = 'invoking',
  WAITING_TO_COMPENSATE = 'waiting_to_compensate',
  COMPENSATING = 'compensating',
  DONE = 'done',
  REVERTED = 'reverted',
  FAILED = 'failed',
  DORMANT = 'dormant',
  SKIPPED = 'skipped',
}

export type TransactionStepsDefinition = {
  action?: string
  /** Next step to execute */
  next?: TransactionStepsDefinition | TransactionStepsDefinition[]
  maxRetries?: number
  continueOnPermanentFailure?: boolean

  /* If true, the workflow will not wait for their sibling steps to complete before moving to the next step.*/
  noWait?: boolean

  /** If true, no compensation action will be triggered for this step in case of a failure.*/
  noCompensation?: boolean

  /**
   * If true, the response of this step will be stored.
   * Default is true.
   */
  forwardResponse?: boolean
}

export enum TransactionHandlerType {
  INVOKE = 'invoke',
  COMPENSATE = 'compensate',
}

export type TransactionFlow = {
  transactionModelId: string
  transactionId: string
  state: TransactionState
  hasSkippedSteps: boolean
  hasFailedSteps: boolean
  startedAt?: number
  steps: {
    [key: string]: TransactionStep
  }
}
