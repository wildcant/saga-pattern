import {
  LocalWorkflow,
  TransactionModelOptions,
  WorkflowHandler,
  WorkflowManager,
} from '../../../orchestrator'
import { OrchestrationUtils } from '../../../utils'
import { ExportedWorkflow, exportWorkflow } from '../../helper'
import { proxify } from './helpers/proxy'
import { CreateWorkflowComposerContext, WorkflowData, WorkflowDataProperties } from './type'

global[OrchestrationUtils.SymbolMedusaWorkflowComposerContext] = null

/**
 * An exported workflow, which is the type of a workflow constructed by the {@link createWorkflow} function. The exported workflow can be invoked to create
 * an executable workflow. So, to execute the workflow, you must invoke the exported workflow, then run the
 * `run` method of the exported workflow.
 */
type ReturnWorkflow<TData, TResult, THooks extends Record<string, Function>> = {
  <TDataOverride = undefined, TResultOverride = undefined>(): Omit<
    LocalWorkflow,
    'run' | 'registerStepSuccess' | 'registerStepFailure'
  > &
    ExportedWorkflow<TData, TResult, TDataOverride, TResultOverride>
} & THooks & {
    getName: () => string
  }

/**
 * This function creates a workflow with the provided name and a constructor function.
 * The constructor function builds the workflow from steps created by the {@link createStep} function.
 * The returned workflow is an exported workflow of type {@link ReturnWorkflow}, meaning it's not executed right away. To execute it,
 * invoke the exported workflow, then run its `run` method.
 *
 * @typeParam TData - The type of the input passed to the composer function.
 * @typeParam TResult - The type of the output returned by the composer function.
 * @typeParam THooks - The type of hooks defined in the workflow.
 *
 * @returns The created workflow. You can later execute the workflow by invoking it, then using its `run` method.
 */

export function createWorkflow<
  TData,
  TResult,
  THooks extends Record<string, Function> = Record<string, Function>,
>(
  /**
   * The name of the workflow.
   */
  name: string,
  /**
   * The constructor function that is executed when the `run` method in {@link ReturnWorkflow} is used.
   * The function can't be an arrow function or an asynchronus function. It also can't directly manipulate data.
   * You'll have to use the {@link transform} function if you need to directly manipulate data.
   */
  composer: (input: WorkflowData<TData>) =>
    | void
    | WorkflowData<TResult>
    | {
        [K in keyof TResult]: WorkflowData<TResult[K]> | WorkflowDataProperties<TResult[K]>
      },
  options?: TransactionModelOptions,
): ReturnWorkflow<TData, TResult, THooks> {
  const handlers: WorkflowHandler = new Map()

  if (WorkflowManager.getWorkflow(name)) {
    WorkflowManager.unregister(name)
  }

  WorkflowManager.register(name, undefined, handlers, options)

  const context: CreateWorkflowComposerContext = {
    workflowId: name,
    flow: WorkflowManager.getTransactionDefinition(name),
    handlers,
    hooks_: [],
    hooksCallback_: {},
    hookBinder: (name, fn) => {
      context.hooks_.push(name)
      return fn(context)
    },
    stepBinder: fn => {
      return fn.bind(context)()
    },
    parallelizeBinder: fn => {
      return fn.bind(context)()
    },
  }

  global[OrchestrationUtils.SymbolMedusaWorkflowComposerContext] = context

  const inputPlaceHolder = proxify<WorkflowData>({
    __type: OrchestrationUtils.SymbolInputReference,
    __step__: '',
  })

  const returnedStep = composer.apply(context, [inputPlaceHolder])

  delete global[OrchestrationUtils.SymbolMedusaWorkflowComposerContext]

  WorkflowManager.update(name, context.flow, handlers)

  const workflow = exportWorkflow<TData, TResult>(name, returnedStep, undefined, {
    wrappedInput: true,
  })

  const mainFlow = <TDataOverride = undefined, TResultOverride = undefined>() => {
    const workflow_ = workflow<TDataOverride, TResultOverride>()

    return workflow_
  }

  let shouldRegisterHookHandler = true

  for (const hook of context.hooks_) {
    mainFlow[hook] = fn => {
      context.hooksCallback_[hook] ??= []

      if (!shouldRegisterHookHandler) {
        console.warn(
          `A hook handler has already been registered for the ${hook} hook. The current handler registration will be skipped.`,
        )
        return
      }

      context.hooksCallback_[hook].push(fn)
      shouldRegisterHookHandler = false
    }
  }

  mainFlow.getName = () => name

  return mainFlow as ReturnWorkflow<TData, TResult, THooks>
}
