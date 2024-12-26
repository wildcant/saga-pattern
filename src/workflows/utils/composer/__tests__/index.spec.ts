import { createStep } from '../create-step'
import { createWorkflow } from '../create-workflow'
import { StepResponse } from '../helpers'
import { WorkflowResponse } from '../helpers/workflow-response'
import { transform } from '../transform'
import { WorkflowData } from '../type'
import { when } from '../when'

let count = 1
const getNewWorkflowId = () => `workflow-${count++}`

describe('Workflow composer', () => {
  describe('running sub workflows', () => {
    it('should succeed', async function () {
      const step1 = createStep('step1', async (_, context) => {
        return new StepResponse({ result: 'step1' })
      })
      const step2 = createStep('step2', async (input: string, context) => {
        return new StepResponse({ result: input })
      })
      const step3 = createStep('step3', async (input: string, context) => {
        return new StepResponse({ result: input })
      })

      const subWorkflow = createWorkflow(
        getNewWorkflowId(),
        function (input: WorkflowData<string>) {
          step1()
          return new WorkflowResponse(step2(input))
        },
      )

      const workflow = createWorkflow(getNewWorkflowId(), function () {
        const subWorkflowRes = subWorkflow.runAsStep({
          input: 'hi from outside',
        })
        return new WorkflowResponse(step3(subWorkflowRes.result))
      })

      const { result } = await workflow.run({ input: {} })

      expect(result).toEqual({ result: 'hi from outside' })
    })

    it('should skip step if condition is false', async function () {
      const step1 = createStep('step1', async (_, context) => {
        return new StepResponse({ result: 'step1' })
      })
      const step2 = createStep('step2', async (input: string, context) => {
        return new StepResponse({ result: input })
      })
      const step3 = createStep('step3', async (input: string | undefined, context) => {
        return new StepResponse({ result: input ?? 'default response' })
      })

      const subWorkflow = createWorkflow(
        getNewWorkflowId(),
        function (input: WorkflowData<string>) {
          step1()
          return new WorkflowResponse(step2(input))
        },
      )

      const workflow = createWorkflow(
        getNewWorkflowId(),
        function (input: { callSubFlow: boolean }) {
          const subWorkflowRes = when({ input }, ({ input }) => {
            return input.callSubFlow
          }).then(() => {
            return subWorkflow.runAsStep({
              input: 'hi from outside',
            })
          })

          return new WorkflowResponse(step3(subWorkflowRes!.result))
        },
      )

      const { result } = await workflow.run({ input: { callSubFlow: false } })

      expect(result).toEqual({ result: 'default response' })
    })

    it('should not skip step if condition is true', async function () {
      const step1 = createStep('step1', async (_, context) => {
        return new StepResponse({ result: 'step1' })
      })
      const step2 = createStep('step2', async (input: string, context) => {
        return new StepResponse({ result: input })
      })
      const step3 = createStep('step3', async (input: string | undefined, context) => {
        return new StepResponse({ result: input ?? 'default response' })
      })

      const subWorkflow = createWorkflow(
        getNewWorkflowId(),
        function (input: WorkflowData<string>) {
          step1()
          return new WorkflowResponse(step2(input))
        },
      )

      const workflow = createWorkflow(
        getNewWorkflowId(),
        function (input: { callSubFlow: boolean }) {
          const subWorkflowRes = when({ input }, ({ input }) => {
            return input.callSubFlow
          }).then(() => {
            return subWorkflow.runAsStep({
              input: 'hi from outside',
            })
          })

          return new WorkflowResponse(step3(subWorkflowRes!.result))
        },
      )

      const { result } = await workflow.run({
        input: { callSubFlow: true },
      })

      expect(result).toEqual({ result: 'hi from outside' })

      const { result: res2 } = await workflow.run({
        input: { callSubFlow: false },
      })

      expect(res2).toEqual({ result: 'default response' })
    })

    it('should not return value if when condition is false', async function () {
      const workflow = createWorkflow(getNewWorkflowId(), function (input: { ret: boolean }) {
        const value = when({ input }, ({ input }) => {
          return input.ret
        }).then(() => {
          return { hasValue: true }
        })

        return new WorkflowResponse(value)
      })

      const { result } = await workflow.run({
        input: { ret: false },
      })

      expect(result).toEqual(undefined)

      const { result: res2 } = await workflow.run({
        input: { ret: true },
      })

      expect(res2).toEqual({ hasValue: true })
    })

    it('should revert the workflow and sub workflow on failure', async function () {
      const step1Mock = jest.fn()
      const step1 = createStep(
        'step1',
        async () => {
          return new StepResponse({ result: 'step1' })
        },
        step1Mock,
      )

      const step2Mock = jest.fn()
      const step2 = createStep(
        'step2',
        async (input: string) => {
          return new StepResponse({ result: input })
        },
        step2Mock,
      )

      const step3Mock = jest.fn()
      const step3 = createStep(
        'step3',
        async () => {
          return new StepResponse()
        },
        step3Mock,
      )

      const step4WithError = createStep('step4', async () => {
        throw new Error('Step4 failed')
      })

      const subWorkflow = createWorkflow(
        getNewWorkflowId(),
        function (input: WorkflowData<string>) {
          step1()
          return new WorkflowResponse(step2(input))
        },
      )

      const workflow = createWorkflow(getNewWorkflowId(), function () {
        step3()
        const subWorkflowRes = subWorkflow.runAsStep({
          input: 'hi from outside',
        })
        step4WithError()
        return new WorkflowResponse(subWorkflowRes)
      })

      const { errors } = await workflow.run({ throwOnError: false })

      expect(errors).toEqual([
        expect.objectContaining({
          error: expect.objectContaining({
            message: 'Step4 failed',
          }),
        }),
      ])

      expect(step1Mock).toHaveBeenCalledTimes(1)
      expect(step2Mock).toHaveBeenCalledTimes(1)
      expect(step3Mock).toHaveBeenCalledTimes(1)
    })

    it('should succeed and pass down the transaction id and event group id when provided from the context', async function () {
      let parentContext, childContext

      const childWorkflowStep1 = createStep('step1', async (_, context) => {
        childContext = context
        return new StepResponse({ result: 'step1' })
      })
      const childWorkflowStep2 = createStep('step2', async (input: string, context) => {
        return new StepResponse({ result: input })
      })
      const step1 = createStep('step3', async (input: string, context) => {
        parentContext = context
        return new StepResponse({ result: input })
      })

      const wfId = getNewWorkflowId()
      const subWorkflow = createWorkflow(wfId, function (input: WorkflowData<string>) {
        childWorkflowStep1()
        return new WorkflowResponse(childWorkflowStep2(input))
      })

      const workflow = createWorkflow(getNewWorkflowId(), function () {
        const subWorkflowRes = subWorkflow.runAsStep({
          input: 'hi from outside',
        })
        return new WorkflowResponse(step1(subWorkflowRes.result))
      })

      const { result } = await workflow.run({
        input: {},
        context: {
          eventGroupId: 'eventGroupId',
          transactionId: 'transactionId',
        },
      })

      expect(result).toEqual({ result: 'hi from outside' })

      expect(parentContext.transactionId).toEqual(expect.any(String))
      expect(childContext.transactionId).toEqual(wfId + '-as-step-' + parentContext.transactionId)

      expect(parentContext.eventGroupId).toEqual('eventGroupId')
      expect(parentContext.eventGroupId).toEqual(childContext.eventGroupId)
    })

    it('should succeed and pass down the transaction id and event group id when not provided from the context', async function () {
      let parentContext, childContext

      const childWorkflowStep1 = createStep('step1', async (_, context) => {
        childContext = context
        return new StepResponse({ result: 'step1' })
      })
      const childWorkflowStep2 = createStep('step2', async (input: string, context) => {
        return new StepResponse({ result: input })
      })
      const step1 = createStep('step3', async (input: string, context) => {
        parentContext = context
        return new StepResponse({ result: input })
      })

      const wfId = getNewWorkflowId()
      const subWorkflow = createWorkflow(wfId, function (input: WorkflowData<string>) {
        childWorkflowStep1()
        return new WorkflowResponse(childWorkflowStep2(input))
      })

      const workflow = createWorkflow(getNewWorkflowId(), function () {
        const subWorkflowRes = subWorkflow.runAsStep({
          input: 'hi from outside',
        })
        return new WorkflowResponse(step1(subWorkflowRes.result))
      })

      const { result } = await workflow.run({
        input: {},
      })

      expect(result).toEqual({ result: 'hi from outside' })

      expect(parentContext.transactionId).toBeTruthy()
      expect(childContext.transactionId).toEqual(wfId + '-as-step-' + parentContext.transactionId)

      expect(parentContext.eventGroupId).toBeTruthy()
      expect(parentContext.eventGroupId).toEqual(childContext.eventGroupId)
    })
  })

  it('should not throw an unhandled error on failed transformer resolution after a step fail, but should rather push the errors in the errors result', async function () {
    const step1 = createStep('step1', async () => {
      return new StepResponse({ result: 'step1' })
    })
    const step2 = createStep('step2', async () => {
      throw new Error('step2 failed')
    })

    const work = createWorkflow('id' as any, () => {
      step1()
      const resStep2 = step2()

      const transformedData = transform({ data: resStep2 }, data => {
        // @ts-expect-error "Since we are reading result from undefined"
        return { result: data.data.result }
      })

      return new WorkflowResponse(
        transform({ data: transformedData, resStep2 }, data => {
          return { result: data.data }
        }),
      )
    })

    const { errors } = await work.run({ input: {}, throwOnError: false })

    expect(errors).toEqual([
      {
        action: 'step2',
        handlerType: 'invoke',
        error: expect.objectContaining({
          message: 'step2 failed',
        }),
      },
      expect.objectContaining({
        message: "Cannot read properties of undefined (reading 'result')",
      }),
    ])
  })
})
