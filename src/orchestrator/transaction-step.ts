import { TransactionState, TransactionStepsDefinition, TransactionStepStatus } from './types'

export type TransactionFlow = {
  transactionModelId: string
  transactionId: string
  state: TransactionState
  startedAt?: number
  steps: {
    [key: string]: TransactionStep
  }
}

/**
 * @classdesc Represents a single transaction step
 */
export class TransactionStep {
  private stepFailed = false

  id: string
  definition: TransactionStepsDefinition
  invoke: {
    status: TransactionStepStatus
    state: TransactionState
  }
  compensate: {
    status: TransactionStepStatus
    state: TransactionState
  }
  next: string[]
  depth: number
  lastAttempt: number | null
  attempts: number
  failures: number
  forwardResponse: boolean
  response: unknown

  public isCompensating() {
    return this.stepFailed
  }

  public beginCompensation() {
    if (this.isCompensating()) {
      return
    }

    this.stepFailed = true
    this.attempts = 0
    this.failures = 0
    this.lastAttempt = null
  }

  public getStates() {
    return this.isCompensating() ? this.compensate : this.invoke
  }

  public changeState(toState: TransactionState) {
    const allowed = {
      [TransactionState.DORMANT]: [TransactionState.NOT_STARTED],
      [TransactionState.NOT_STARTED]: [
        TransactionState.INVOKING,
        TransactionState.COMPENSATING,
        TransactionState.FAILED,
        TransactionState.SKIPPED,
      ],
      [TransactionState.INVOKING]: [TransactionState.FAILED, TransactionState.DONE],
      [TransactionState.COMPENSATING]: [TransactionState.REVERTED, TransactionState.FAILED],
      [TransactionState.DONE]: [TransactionState.COMPENSATING],
    }

    const curState = this.getStates()
    if (curState.state === toState || allowed?.[curState.state]?.includes(toState)) {
      curState.state = toState
      return
    }

    throw new Error(`Updating State from "${curState.state}" to "${toState}" is not allowed.`)
  }

  public changeStatus(toStatus: TransactionStepStatus) {
    const allowed = {
      [TransactionStepStatus.WAITING]: [
        TransactionStepStatus.OK,
        TransactionStepStatus.TEMPORARY_FAILURE,
        TransactionStepStatus.PERMANENT_FAILURE,
      ],
      [TransactionStepStatus.TEMPORARY_FAILURE]: [
        TransactionStepStatus.IDLE,
        TransactionStepStatus.PERMANENT_FAILURE,
      ],
      [TransactionStepStatus.PERMANENT_FAILURE]: [TransactionStepStatus.IDLE],
    }

    const curState = this.getStates()
    if (
      curState.status === toStatus ||
      toStatus === TransactionStepStatus.WAITING ||
      allowed?.[curState.status]?.includes(toStatus)
    ) {
      curState.status = toStatus
      return
    }

    throw new Error(`Updating Status from "${curState.status}" to "${toStatus}" is not allowed.`)
  }

  public getResponse(): unknown {
    return this.response
  }

  public saveResponse(response) {
    this.response = response
  }

  // canRetry(): boolean {
  //   return !!(
  //     this.lastAttempt &&
  //     this.definition.retryInterval &&
  //     Date.now() - this.lastAttempt > this.definition.retryInterval * 1e3
  //   )
  // }

  canInvoke(flowState: TransactionState): boolean {
    const { status, state } = this.getStates()
    return (
      (!this.isCompensating() &&
        state === TransactionState.NOT_STARTED &&
        flowState === TransactionState.INVOKING) ||
      status === TransactionStepStatus.TEMPORARY_FAILURE
    )
  }

  canCompensate(flowState: TransactionState): boolean {
    return (
      this.isCompensating() &&
      this.getStates().state === TransactionState.NOT_STARTED &&
      flowState === TransactionState.COMPENSATING
    )
  }
}
