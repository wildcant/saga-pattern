import {
  DistributedTransaction,
  TransactionPayload,
  TransactionStepHandler,
} from './distributed-transaction'
import { TransactionStep } from './transaction-step'
import {
  TransactionFlow,
  TransactionHandlerType,
  TransactionState,
  TransactionStepsDefinition,
  TransactionStepStatus,
} from './types'

export class TransactionOrchestrator {
  private ROOT_STEP = '_root'
  private invokeSteps: string[] = []
  private compensateSteps: string[] = []
  public DEFAULT_RETRIES = 3

  constructor(private id: string, private definition: TransactionStepsDefinition) {}

  private buildSteps(
    flow: TransactionStepsDefinition,
    existingSteps?: { [key: string]: TransactionStep },
  ): { [key: string]: TransactionStep } {
    const states: { [key: string]: TransactionStep } = {
      [this.ROOT_STEP]: {
        id: this.ROOT_STEP,
        next: [] as string[],
      } as unknown as TransactionStep,
    }

    const actionNames = new Set<string>()
    const queue: any[] = [{ obj: flow, level: [this.ROOT_STEP] }]

    while (queue.length > 0) {
      const { obj, level } = queue.shift()

      for (const key in obj) {
        // eslint-disable-next-line no-prototype-builtins
        if (!obj.hasOwnProperty(key)) {
          continue
        }

        if (typeof obj[key] === 'object' && obj[key] !== null) {
          queue.push({ obj: obj[key], level: [...level] })
        } else if (key === 'action') {
          if (actionNames.has(obj.action)) {
            throw new Error(`Action "${obj.action}" is already defined.`)
          }

          actionNames.add(obj.action)
          level.push(obj.action)
          const id = level.join('.')
          const parent = level.slice(0, level.length - 1).join('.')
          states[parent].next?.push(id)

          const definitionCopy = { ...obj }
          delete definitionCopy.next

          states[id] = Object.assign(
            new TransactionStep(),
            existingSteps?.[id] || {
              id,
              depth: level.length - 1,
              definition: definitionCopy,
              forwardResponse: definitionCopy.forwardResponse ?? true,
              invoke: {
                state: TransactionState.NOT_STARTED,
                status: TransactionStepStatus.IDLE,
              },
              compensate: {
                state: TransactionState.DORMANT,
                status: TransactionStepStatus.IDLE,
              },
              attempts: 0,
              failures: 0,
              lastAttempt: null,
              next: [],
            },
          )
        }
      }
    }

    return states
  }

  private createTransactionFlow(transactionId: string) {
    const model: TransactionFlow = {
      transactionModelId: this.id,
      transactionId,
      state: TransactionState.NOT_STARTED,
      steps: this.buildSteps(this.definition),
      hasFailedSteps: false,
      hasSkippedSteps: false,
    }
    return model
  }

  /** Create a new transaction
   * @param transactionId - unique identifier of the transaction
   * @param handler - function to handle action of the transaction
   * @param payload - payload to be passed to all the transaction steps
   */
  public async beginTransaction(
    transactionId: string,
    handler: TransactionStepHandler,
    payload?: unknown,
  ): Promise<DistributedTransaction> {
    const modelFlow = this.createTransactionFlow(transactionId)

    const transaction = new DistributedTransaction(modelFlow, handler, payload)

    return transaction
  }
  private getCompensationSteps(flow: TransactionFlow): string[] {
    if (this.compensateSteps.length) {
      return this.compensateSteps
    }

    const steps = Object.keys(flow.steps)
    steps.sort((a, b) => (flow.steps[b].depth || 0) - (flow.steps[a].depth || 0))
    this.compensateSteps = steps

    return steps
  }

  private getInvokeSteps(flow: TransactionFlow): string[] {
    if (this.invokeSteps.length) {
      return this.invokeSteps
    }

    const steps = Object.keys(flow.steps)

    steps.sort((a, b) => flow.steps[a].depth - flow.steps[b].depth)
    this.invokeSteps = steps

    return steps
  }

  private canMoveBackward(flow: TransactionFlow, step: TransactionStep) {
    const states = [
      TransactionState.DONE,
      TransactionState.REVERTED,
      TransactionState.FAILED,
      TransactionState.DORMANT,
    ]
    const siblings = step.next.map(sib => flow.steps[sib])
    return siblings.length === 0 || siblings.every(sib => states.includes(sib.compensate.state))
  }

  private canMoveForward(flow: TransactionFlow, previousStep: TransactionStep) {
    const states = [TransactionState.DONE, TransactionState.FAILED, TransactionState.SKIPPED]

    const siblings = this.getPreviousStep(flow, previousStep).next.map(sib => flow.steps[sib])

    return (
      !!previousStep.definition.noWait || siblings.every(sib => states.includes(sib.invoke.state))
    )
  }

  private canContinue(flow: TransactionFlow, step: TransactionStep): boolean {
    if (flow.state == TransactionState.COMPENSATING) {
      return this.canMoveBackward(flow, step)
    } else {
      const previous = this.getPreviousStep(flow, step)
      if (previous.id === this.ROOT_STEP) {
        return true
      }

      return this.canMoveForward(flow, previous)
    }
  }

  private flagStepsToRevert(flow: TransactionFlow): void {
    for (const step in flow.steps) {
      if (step === this.ROOT_STEP) {
        continue
      }

      const stepDef = flow.steps[step]
      const curState = stepDef.getStates()
      if (
        (curState.state === TransactionState.DONE ||
          curState.status === TransactionStepStatus.PERMANENT_FAILURE) &&
        !stepDef.definition.noCompensation
      ) {
        stepDef.beginCompensation()
        stepDef.changeState(TransactionState.NOT_STARTED)
      }
    }
  }

  private checkAllSteps(transaction: DistributedTransaction): {
    next: TransactionStep[]
    total: number
    remaining: number
    completed: number
  } {
    let hasSkipped = false
    let hasIgnoredFailure = false
    let hasFailed = false
    let hasWaiting = false
    let hasReverted = false
    let completedSteps = 0

    const flow = transaction.getFlow()

    const nextSteps: TransactionStep[] = []
    const allSteps =
      flow.state === TransactionState.COMPENSATING
        ? this.getCompensationSteps(flow)
        : this.getInvokeSteps(flow)

    for (const step of allSteps) {
      if (step === this.ROOT_STEP || !this.canContinue(flow, flow.steps[step])) {
        continue
      }

      const stepDef = flow.steps[step]
      const curState = stepDef.getStates()

      if (curState.status === TransactionStepStatus.WAITING) {
        hasWaiting = true
        // if (stepDef.canRetry()) {
        //   nextSteps.push(stepDef)
        // }
        continue
      }

      if (stepDef.canInvoke(flow.state) || stepDef.canCompensate(flow.state)) {
        nextSteps.push(stepDef)
      } else {
        completedSteps++

        if (curState.state === TransactionState.SKIPPED) {
          hasSkipped = true
        } else if (curState.state === TransactionState.REVERTED) {
          hasReverted = true
        } else if (curState.state === TransactionState.FAILED) {
          if (stepDef.definition.continueOnPermanentFailure) {
            hasIgnoredFailure = true
          } else {
            hasFailed = true
          }
        }
      }
    }

    const totalSteps = allSteps.length - 1
    if (
      flow.state === TransactionState.WAITING_TO_COMPENSATE &&
      nextSteps.length === 0 &&
      !hasWaiting
    ) {
      flow.state = TransactionState.COMPENSATING
      this.flagStepsToRevert(flow)

      // this.emit('compensate', transaction)

      return this.checkAllSteps(transaction)
    } else if (completedSteps === totalSteps) {
      if (hasSkipped) {
        flow.hasSkippedSteps = true
      }
      if (hasIgnoredFailure) {
        flow.hasFailedSteps = true
      }
      if (hasFailed) {
        flow.state = TransactionState.FAILED
      } else {
        flow.state = hasReverted ? TransactionState.REVERTED : TransactionState.DONE
      }

      // this.emit('finish', transaction)

      // void transaction.deleteCheckpoint()
    }

    return {
      next: nextSteps,
      total: totalSteps,
      remaining: totalSteps - completedSteps,
      completed: completedSteps,
    }
  }

  private getPreviousStep(flow: TransactionFlow, step: TransactionStep) {
    const id = step.id.split('.')
    id.pop()
    const parentId = id.join('.')
    return flow.steps[parentId]
  }

  private async setStepSuccess(step: TransactionStep, response: unknown): Promise<void> {
    if (step.forwardResponse) {
      step.saveResponse(response)
    }

    step.changeStatus(TransactionStepStatus.OK)

    if (step.isCompensating()) {
      step.changeState(TransactionState.REVERTED)
    } else {
      step.changeState(TransactionState.DONE)
    }

    // if (step.definition.async) {
    //   await transaction.saveCheckpoint()
    // }
  }

  private async setStepFailure(
    transaction: DistributedTransaction,
    step: TransactionStep,
    error: Error | null,
    maxRetries: number = this.DEFAULT_RETRIES,
  ): Promise<void> {
    step.failures++

    step.changeStatus(TransactionStepStatus.TEMPORARY_FAILURE)

    if (step.failures > maxRetries) {
      step.changeState(TransactionState.FAILED)
      step.changeStatus(TransactionStepStatus.PERMANENT_FAILURE)

      transaction.addError(
        step.definition.action!,
        step.isCompensating() ? TransactionHandlerType.COMPENSATE : TransactionHandlerType.INVOKE,
        error,
      )

      if (!step.isCompensating()) {
        const flow = transaction.getFlow()
        if (step.definition.continueOnPermanentFailure) {
          for (const childStep of step.next) {
            const child = flow.steps[childStep]
            child.changeState(TransactionState.SKIPPED)
          }
        } else {
          flow.state = TransactionState.WAITING_TO_COMPENSATE
        }
      }
    }

    // if (step.definition.async) {
    //   await transaction.saveCheckpoint()
    // }
  }

  private async executeNext(transaction: DistributedTransaction): Promise<void> {
    if (transaction.hasFinished()) {
      return
    }

    const flow = transaction.getFlow()
    const nextSteps = this.checkAllSteps(transaction)
    const execution: Promise<void | unknown>[] = []

    for (const step of nextSteps.next) {
      const curState = step.getStates()
      const type = step.isCompensating()
        ? TransactionHandlerType.COMPENSATE
        : TransactionHandlerType.INVOKE

      step.lastAttempt = Date.now()
      step.attempts++

      if (curState.state === TransactionState.NOT_STARTED) {
        if (step.isCompensating()) {
          step.changeState(TransactionState.COMPENSATING)
        } else if (flow.state === TransactionState.INVOKING) {
          step.changeState(TransactionState.INVOKING)
        }
      }

      step.changeStatus(TransactionStepStatus.WAITING)

      const parent = this.getPreviousStep(flow, step)
      let payloadData = transaction.payload

      if (parent.forwardResponse) {
        if (!payloadData) {
          payloadData = {}
        }

        payloadData._response = parent.getResponse()
      }

      const payload = new TransactionPayload(
        {
          producer: flow.transactionModelId,
          reply_to_topic: TransactionOrchestrator.getKeyName('trans', flow.transactionModelId),
          idempotency_key: TransactionOrchestrator.getKeyName(
            flow.transactionId,
            step.definition.action!,
            type,
          ),
          action: step.definition.action + '',
          action_type: type,
          attempt: step.attempts,
          timestamp: Date.now(),
        },
        payloadData,
      )

      execution.push(
        transaction
          .handler(step.definition.action + '', type, payload)
          .then(async response => {
            await this.setStepSuccess(step, response)
          })
          .catch(async error => {
            await this.setStepFailure(transaction, step, error, step.definition.maxRetries)
          }),
      )
    }

    await Promise.all(execution)

    if (nextSteps.next.length > 0) {
      await this.executeNext(transaction)
    }
  }

  /**
   * Start a new transaction or resume a transaction that has been previously started
   * @param transaction - The transaction to resume
   */
  public async resume(transaction: DistributedTransaction): Promise<void> {
    if (transaction.modelId !== this.id) {
      throw new Error(
        `TransactionModel "${transaction.modelId}" cannot be orchestrated by "${this.id}" model.`,
      )
    }

    if (transaction.hasFinished()) {
      return
    }

    const flow = transaction.getFlow()

    if (flow.state === TransactionState.NOT_STARTED) {
      flow.state = TransactionState.INVOKING
    }

    await this.executeNext(transaction)
  }

  /***** Static methods *****/
  private static SEPARATOR = ':'
  public static getKeyName(...params: string[]): string {
    return params.join(this.SEPARATOR)
  }
}
