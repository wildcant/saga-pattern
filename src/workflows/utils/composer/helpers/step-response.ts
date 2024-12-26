import { PermanentStepFailureError, SkipStepResponse } from '../../../../orchestrator'
import { OrchestrationUtils, isDefined } from '../../../../utils'

/**
 * This class is used to create the response returned by a step. A step return its data by returning an instance of `StepResponse`.
 *
 * @typeParam TOutput - The type of the output of the step.
 * @typeParam TCompensateInput -
 * The type of the compensation input. If the step doesn't specify any compensation input, then the type of `TCompensateInput` is the same
 * as that of `TOutput`.
 */
export class StepResponse<TOutput, TCompensateInput = TOutput> {
  readonly #__type = OrchestrationUtils.SymbolWorkflowStepResponse
  readonly #output: TOutput
  readonly #compensateInput?: TCompensateInput

  /**
   * The constructor of the StepResponse
   *
   * @typeParam TOutput - The type of the output of the step.
   * @typeParam TCompensateInput -
   * The type of the compensation input. If the step doesn't specify any compensation input, then the type of `TCompensateInput` is the same
   * as that of `TOutput`.
   */
  constructor(
    /**
     * The output of the step.
     */
    output?: TOutput,
    /**
     * The input to be passed as a parameter to the step's compensation function. If not provided, the `output` will be provided instead.
     */
    compensateInput?: TCompensateInput,
  ) {
    if (isDefined(output)) {
      this.#output = output
    }
    this.#compensateInput = (compensateInput ?? output) as TCompensateInput
  }

  /**
   * Creates a StepResponse that indicates that the step has failed and the retry mechanism should not kick in anymore.
   *
   * @param message - An optional message to be logged.
   */
  static permanentFailure(message = 'Permanent failure', compensateInput?: unknown): never {
    const response = isDefined(compensateInput) ? new StepResponse(compensateInput) : undefined
    throw new PermanentStepFailureError(message, response)
  }

  static skip(): SkipStepResponse {
    return new SkipStepResponse()
  }

  /**
   * @internal
   */
  get __type() {
    return this.#__type
  }

  /**
   * @internal
   */
  get output(): TOutput {
    return this.#output
  }

  /**
   * @internal
   */
  get compensateInput(): TCompensateInput {
    return this.#compensateInput as TCompensateInput
  }

  /**
   * @internal
   */
  toJSON() {
    return {
      __type: this.#__type,
      output: this.#output,
      compensateInput: this.#compensateInput,
    }
  }
}
