import { OrchestrationUtils } from '../../../utils'
import { resolveValue } from './helpers'
import { CreateWorkflowComposerContext, StepExecutionContext, WorkflowData } from './type'

/**
 *
 * @ignore
 *
 * This function allows you to add hooks in your workflow that provide access to some data. Then, consumers of that workflow can add a handler function that performs
 * an action with the provided data or modify it.
 *
 * For example, in a "create product" workflow, you may add a hook after the product is created, providing access to the created product.
 * Then, developers using that workflow can hook into that point to access the product, modify its attributes, then return the updated product.
 *
 * @typeParam TOutput - The expected output of the hook's handler function.
 * @returns The output of handler functions of this hook. If there are no handler functions, the output is `undefined`.
 */
export function hook<TOutput>(
  /**
   * The name of the hook. This will be used by the consumer to add a handler method for the hook.
   */
  name: string,
  /**
   * The data that a handler function receives as a parameter.
   */
  value: any,
): WorkflowData<TOutput> {
  const hookBinder = (
    global[OrchestrationUtils.SymbolMedusaWorkflowComposerContext] as CreateWorkflowComposerContext
  ).hookBinder

  return hookBinder(name, function (context) {
    return {
      __value: async function (transactionContext) {
        const executionContext: StepExecutionContext = {
          metadata: transactionContext.metadata,
        }

        const allValues = await resolveValue(value, transactionContext)
        const stepValue = allValues ? JSON.parse(JSON.stringify(allValues)) : allValues

        let finalResult
        const functions = context.hooksCallback_[name]
        for (let i = 0; i < functions.length; i++) {
          const fn = functions[i]
          const arg = i === 0 ? stepValue : finalResult
          finalResult = await fn.apply(fn, [arg, executionContext])
        }
        return finalResult
      },
      __type: OrchestrationUtils.SymbolWorkflowHook,
    }
  })
}
