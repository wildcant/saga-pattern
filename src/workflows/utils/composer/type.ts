import {
  OrchestratorBuilder,
  TransactionContext as OriginalWorkflowTransactionContext,
  TransactionPayload,
  TransactionStepsDefinition,
  WorkflowHandler,
} from '../../../orchestrator'

export type StepFunctionResult<TOutput extends unknown | unknown[] = unknown> = (
  this: CreateWorkflowComposerContext,
) => WorkflowData<{ [K in keyof TOutput]: TOutput[K] }>

/**
 * A step function to be used in a workflow.
 *
 * @typeParam TInput - The type of the input of the step.
 * @typeParam TOutput - The type of the output of the step.
 */
export type StepFunction<TInput, TOutput = unknown> = (keyof TInput extends []
  ? // Function that doesn't expect any input
    {
      (): WorkflowData<{
        [K in keyof TOutput]: TOutput[K]
      }>
    }
  : // function that expects an input object
    {
      (
        input: TInput extends object
          ? { [K in keyof TInput]: WorkflowData<TInput[K]> | TInput[K] }
          : WorkflowData<TInput> | TInput,
      ): WorkflowData<{
        [K in keyof TOutput]: TOutput[K]
      }>
    }) &
  WorkflowDataProperties<{
    [K in keyof TOutput]: TOutput[K]
  }> & {
    config(config: Pick<TransactionStepsDefinition, 'maxRetries'>): WorkflowData<{
      [K in keyof TOutput]: TOutput[K]
    }>
  } & WorkflowDataProperties<{
    [K in keyof TOutput]: TOutput[K]
  }>

export type WorkflowDataProperties<T = unknown> = {
  __type: Symbol
  __step__: string
}

/**
 * This type is used to encapsulate the input or output type of all utils.
 *
 * @typeParam T - The type of a step's input or result.
 */
export type WorkflowData<T = unknown> = (T extends object
  ? {
      [Key in keyof T]: WorkflowData<T[Key]>
    }
  : WorkflowDataProperties<T>) &
  WorkflowDataProperties<T>

export type CreateWorkflowComposerContext = {
  hooks_: string[]
  hooksCallback_: Record<string, Function[]>
  workflowId: string
  flow: OrchestratorBuilder
  handlers: WorkflowHandler
  stepBinder: <TOutput = unknown>(fn: StepFunctionResult) => WorkflowData<TOutput>
  hookBinder: <TOutput = unknown>(name: string, fn: Function) => WorkflowData<TOutput>
  parallelizeBinder: <TOutput extends WorkflowData[] = WorkflowData[]>(
    fn: (this: CreateWorkflowComposerContext) => TOutput,
  ) => TOutput
}

/**
 * The step's context.
 */
export interface StepExecutionContext {
  /**
   * Metadata passed in the input.
   */
  metadata: TransactionPayload['metadata']
}

export type WorkflowTransactionContext = StepExecutionContext &
  OriginalWorkflowTransactionContext & {
    invoke: { [key: string]: { output: any } }
  }
