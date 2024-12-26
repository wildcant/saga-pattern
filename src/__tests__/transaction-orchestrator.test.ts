import { TransactionPayload } from '../orchestrator/distributed-transaction'
import { TransactionOrchestrator } from '../orchestrator/transaction-orchestrator'
import {
  TransactionHandlerType,
  TransactionState,
  TransactionStepsDefinition,
} from '../orchestrator/types'

describe('Transaction Orchestrator', () => {
  test('1', async () => {
    const mocks = {
      one: jest.fn().mockImplementation(payload => {
        return payload
      }),
      two: jest.fn().mockImplementation(payload => {
        return payload
      }),
    }

    async function handler(
      actionId: string,
      functionHandlerType: TransactionHandlerType,
      payload: TransactionPayload,
    ) {
      const command = {
        firstMethod: {
          [TransactionHandlerType.INVOKE]: () => {
            mocks.one(payload)
          },
        },
        secondMethod: {
          [TransactionHandlerType.INVOKE]: () => {
            mocks.two(payload)
          },
        },
      }
      return command[actionId][functionHandlerType](payload)
    }

    const flow: TransactionStepsDefinition = {
      next: {
        action: 'firstMethod',
        next: {
          action: 'secondMethod',
        },
      },
    }

    const strategy = new TransactionOrchestrator('transaction-name', flow)

    const transaction = await strategy.beginTransaction('transaction_id_123', handler)

    await strategy.resume(transaction)

    expect(transaction.transactionId).toBe('transaction_id_123')
    expect(transaction.getState()).toBe(TransactionState.DONE)
  })

  it("Should run steps in parallel if 'next' is an array", async () => {
    const actionOrder: string[] = []
    async function handler(
      actionId: string,
      functionHandlerType: TransactionHandlerType,
      payload: unknown,
    ) {
      return actionOrder.push(actionId)
    }

    const flow: TransactionStepsDefinition = {
      next: [
        {
          action: 'one',
        },
        {
          action: 'two',
          next: {
            action: 'four',
            next: {
              action: 'six',
            },
          },
        },
        {
          action: 'three',
          next: {
            action: 'five',
          },
        },
      ],
    }

    const strategy = new TransactionOrchestrator('transaction-name', flow)

    const transaction = await strategy.beginTransaction('transaction_id_123', handler)

    await strategy.resume(transaction)
    expect(actionOrder).toEqual(['one', 'two', 'three', 'four', 'five', 'six'])
  })

  it('Should not execute next steps when a step fails', async () => {
    const actionOrder: string[] = []
    async function handler(
      actionId: string,
      functionHandlerType: TransactionHandlerType,
      payload: TransactionPayload,
    ) {
      if (functionHandlerType === TransactionHandlerType.INVOKE) {
        actionOrder.push(actionId)
      }

      if (functionHandlerType === TransactionHandlerType.INVOKE && actionId === 'three') {
        throw new Error()
      }
    }

    const flow: TransactionStepsDefinition = {
      next: [
        {
          action: 'one',
        },
        {
          action: 'two',
          next: {
            action: 'four',
            next: {
              action: 'six',
            },
          },
        },
        {
          action: 'three',
          maxRetries: 0,
          next: {
            action: 'five',
          },
        },
      ],
    }

    const strategy = new TransactionOrchestrator('transaction-name', flow)

    const transaction = await strategy.beginTransaction('transaction_id_123', handler)

    await strategy.resume(transaction)
    expect(actionOrder).toEqual(['one', 'two', 'three'])
  })

  it("Should forward step response if flag 'forwardResponse' is set to true", async () => {
    const mocks = {
      one: jest.fn().mockImplementation(data => {
        return { abc: 1234 }
      }),
      two: jest.fn().mockImplementation(data => {
        return { def: '567' }
      }),
      three: jest.fn().mockImplementation(data => {
        return { end: true }
      }),
    }

    async function handler(
      actionId: string,
      functionHandlerType: TransactionHandlerType,
      payload: TransactionPayload,
    ) {
      const command = {
        firstMethod: {
          [TransactionHandlerType.INVOKE]: data => {
            return mocks.one(data)
          },
        },
        secondMethod: {
          [TransactionHandlerType.INVOKE]: data => {
            return mocks.two(data)
          },
        },
        thirdMethod: {
          [TransactionHandlerType.INVOKE]: data => {
            return mocks.three(data)
          },
        },
      }

      return command[actionId][functionHandlerType]({ ...payload.data })
    }

    const flow: TransactionStepsDefinition = {
      next: {
        action: 'firstMethod',
        forwardResponse: true,
        next: {
          action: 'secondMethod',
          forwardResponse: true,
          next: {
            action: 'thirdMethod',
          },
        },
      },
    }

    const strategy = new TransactionOrchestrator('transaction-name', flow)

    const transaction = await strategy.beginTransaction('transaction_id_123', handler, {
      prop: 123,
    })

    await strategy.resume(transaction)

    expect(mocks.one).toHaveBeenCalledWith({ prop: 123 })

    expect(mocks.two).toHaveBeenCalledWith({ prop: 123, _response: { abc: 1234 } })

    expect(mocks.three).toHaveBeenCalledWith({ prop: 123, _response: { def: '567' } })
  })

  it("Should continue the exection of next steps without waiting for the execution of all its parents when flag 'noWait' is set to true", async () => {
    const actionOrder: string[] = []
    async function handler(
      actionId: string,
      functionHandlerType: TransactionHandlerType,
      payload: TransactionPayload,
    ) {
      if (functionHandlerType === TransactionHandlerType.INVOKE) {
        actionOrder.push(actionId)
      }

      if (functionHandlerType === TransactionHandlerType.INVOKE && actionId === 'three') {
        throw new Error()
      }
    }

    const flow: TransactionStepsDefinition = {
      next: [
        {
          action: 'one',
          next: {
            action: 'five',
          },
        },
        {
          action: 'two',
          noWait: true,
          next: {
            action: 'four',
          },
        },
        {
          action: 'three',
          maxRetries: 0,
        },
      ],
    }

    const strategy = new TransactionOrchestrator('transaction-name', flow)

    const transaction = await strategy.beginTransaction('transaction_id_123', handler)

    strategy.resume(transaction)

    expect(actionOrder).toEqual(['one', 'two', 'three', 'four'])
  })

  it('Should retry steps X times when a step fails and compensate steps afterward', async () => {
    const mocks = {
      one: jest.fn().mockImplementation(payload => {
        return payload
      }),
      compensateOne: jest.fn().mockImplementation(payload => {
        return payload
      }),
      two: jest.fn().mockImplementation(payload => {
        throw new Error()
      }),
      compensateTwo: jest.fn().mockImplementation(payload => {
        return payload
      }),
    }

    async function handler(
      actionId: string,
      functionHandlerType: TransactionHandlerType,
      payload: TransactionPayload,
    ) {
      const command = {
        firstMethod: {
          [TransactionHandlerType.INVOKE]: () => {
            mocks.one(payload)
          },
          [TransactionHandlerType.COMPENSATE]: () => {
            mocks.compensateOne(payload)
          },
        },
        secondMethod: {
          [TransactionHandlerType.INVOKE]: () => {
            mocks.two(payload)
          },
          [TransactionHandlerType.COMPENSATE]: () => {
            mocks.compensateTwo(payload)
          },
        },
      }

      return command[actionId][functionHandlerType](payload)
    }

    const flow: TransactionStepsDefinition = {
      next: {
        action: 'firstMethod',
        next: {
          action: 'secondMethod',
        },
      },
    }

    const strategy = new TransactionOrchestrator('transaction-name', flow)

    const transaction = await strategy.beginTransaction('transaction_id_123', handler)

    await strategy.resume(transaction)

    expect(transaction.transactionId).toBe('transaction_id_123')
    expect(mocks.one).toHaveBeenCalledTimes(1)
    expect(mocks.two).toHaveBeenCalledTimes(1 + strategy.DEFAULT_RETRIES)
    expect(transaction.getState()).toBe(TransactionState.REVERTED)
    expect(mocks.compensateOne).toHaveBeenCalledTimes(1)
    expect(mocks.compensateTwo).toHaveBeenCalledTimes(1)

    expect(mocks.two).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        metadata: expect.objectContaining({
          attempt: 1,
        }),
      }),
    )

    expect(mocks.two).toHaveBeenNthCalledWith(
      4,
      expect.objectContaining({
        metadata: expect.objectContaining({
          attempt: 4,
        }),
      }),
    )
  })

  it('Should fail a transaction if any step fails after retrying X time to compensate it', async () => {
    const mocks = {
      one: jest.fn().mockImplementation(payload => {
        throw new Error()
      }),
    }

    async function handler(
      actionId: string,
      functionHandlerType: TransactionHandlerType,
      payload: TransactionPayload,
    ) {
      const command = {
        firstMethod: {
          [TransactionHandlerType.INVOKE]: () => {
            mocks.one(payload)
          },
        },
      }

      return command[actionId][functionHandlerType](payload)
    }

    const flow: TransactionStepsDefinition = {
      next: {
        action: 'firstMethod',
      },
    }

    const strategy = new TransactionOrchestrator('transaction-name', flow)

    const transaction = await strategy.beginTransaction('transaction_id_123', handler)

    await strategy.resume(transaction)

    expect(mocks.one).toHaveBeenCalledTimes(1 + strategy.DEFAULT_RETRIES)
    expect(transaction.getState()).toBe(TransactionState.FAILED)
  })

  it("Should complete a transaction if a failing step has the flag 'continueOnPermanentFailure' set to true", async () => {
    const mocks = {
      one: jest.fn().mockImplementation(payload => {
        return
      }),
      two: jest.fn().mockImplementation(payload => {
        throw new Error()
      }),
    }

    async function handler(
      actionId: string,
      functionHandlerType: TransactionHandlerType,
      payload: TransactionPayload,
    ) {
      const command = {
        firstMethod: {
          [TransactionHandlerType.INVOKE]: () => {
            mocks.one(payload)
          },
        },
        secondMethod: {
          [TransactionHandlerType.INVOKE]: () => {
            mocks.two(payload)
          },
        },
      }

      return command[actionId][functionHandlerType](payload)
    }

    const flow: TransactionStepsDefinition = {
      next: {
        action: 'firstMethod',
        next: {
          action: 'secondMethod',
          maxRetries: 1,
          continueOnPermanentFailure: true,
        },
      },
    }

    const strategy = new TransactionOrchestrator('transaction-name', flow)

    const transaction = await strategy.beginTransaction('transaction_id_123', handler)

    await strategy.resume(transaction)

    expect(transaction.transactionId).toBe('transaction_id_123')
    expect(mocks.one).toHaveBeenCalledTimes(1)
    expect(mocks.two).toHaveBeenCalledTimes(2)
    expect(transaction.getState()).toBe(TransactionState.DONE)
    expect(transaction.isPartiallyCompleted).toBe(true)
  })
})
