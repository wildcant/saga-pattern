import { OrchestrationUtils } from '../../../utils'
import { CreateWorkflowComposerContext, WorkflowData } from './type'

/**
 * This function is used to run multiple steps in parallel. The result of each step will be returned as part of the result array.
 *
 * @typeParam TResult - The type of the expected result.
 *
 * @returns The step results. The results are ordered in the array by the order they're passed in the function's parameter.
 */
export function parallelize<TResult extends WorkflowData[]>(...steps: TResult): TResult {
  if (!global[OrchestrationUtils.SymbolMedusaWorkflowComposerContext]) {
    throw new Error('parallelize must be used inside a createWorkflow definition')
  }

  const parallelizeBinder = (
    global[OrchestrationUtils.SymbolMedusaWorkflowComposerContext] as CreateWorkflowComposerContext
  ).parallelizeBinder

  const resultSteps = steps.map(step => step)

  return parallelizeBinder<TResult>(function (this: CreateWorkflowComposerContext) {
    const stepOntoMerge = steps.shift()!
    this.flow.mergeActions(stepOntoMerge.__step__, ...steps.map(step => step.__step__))

    return resultSteps as unknown as TResult
  })
}
