import {
  DistributedTransaction,
  TransactionCheckpoint,
} from "../distributed-transaction"
import { TransactionStep } from "../transaction-step"
import { TransactionModelOptions } from "../types"

export interface IDistributedTransactionStorage {
  get(key: string): Promise<TransactionCheckpoint | undefined>
  list(): Promise<TransactionCheckpoint[]>
  save(key: string, data: TransactionCheckpoint, ttl?: number): Promise<void>
  delete(key: string): Promise<void>
  archive(key: string, options?: TransactionModelOptions): Promise<void>
  scheduleRetry(
    transaction: DistributedTransaction,
    step: TransactionStep,
    timestamp: number,
    interval: number
  ): Promise<void>
  clearRetry(
    transaction: DistributedTransaction,
    step: TransactionStep
  ): Promise<void>
  scheduleTransactionTimeout(
    transaction: DistributedTransaction,
    timestamp: number,
    interval: number
  ): Promise<void>
  scheduleStepTimeout(
    transaction: DistributedTransaction,
    step: TransactionStep,
    timestamp: number,
    interval: number
  ): Promise<void>
  clearTransactionTimeout(transaction: DistributedTransaction): Promise<void>
  clearStepTimeout(
    transaction: DistributedTransaction,
    step: TransactionStep
  ): Promise<void>
}

export abstract class DistributedTransactionStorage
  implements IDistributedTransactionStorage
{
  constructor() {
    /* noop */
  }

  async get(key: string): Promise<TransactionCheckpoint | undefined> {
    throw new Error("Method 'get' not implemented.")
  }

  async list(): Promise<TransactionCheckpoint[]> {
    throw new Error("Method 'list' not implemented.")
  }

  async save(
    key: string,
    data: TransactionCheckpoint,
    ttl?: number
  ): Promise<void> {
    throw new Error("Method 'save' not implemented.")
  }

  async delete(key: string): Promise<void> {
    throw new Error("Method 'delete' not implemented.")
  }

  async archive(key: string, options?: TransactionModelOptions): Promise<void> {
    throw new Error("Method 'archive' not implemented.")
  }

  async scheduleRetry(
    transaction: DistributedTransaction,
    step: TransactionStep,
    timestamp: number,
    interval: number
  ): Promise<void> {
    throw new Error("Method 'scheduleRetry' not implemented.")
  }

  async clearRetry(
    transaction: DistributedTransaction,
    step: TransactionStep
  ): Promise<void> {
    throw new Error("Method 'clearRetry' not implemented.")
  }

  async scheduleTransactionTimeout(
    transaction: DistributedTransaction,
    timestamp: number,
    interval: number
  ): Promise<void> {
    throw new Error("Method 'scheduleTransactionTimeout' not implemented.")
  }

  async clearTransactionTimeout(
    transaction: DistributedTransaction
  ): Promise<void> {
    throw new Error("Method 'clearTransactionTimeout' not implemented.")
  }

  async scheduleStepTimeout(
    transaction: DistributedTransaction,
    step: TransactionStep,
    timestamp: number,
    interval: number
  ): Promise<void> {
    throw new Error("Method 'scheduleStepTimeout' not implemented.")
  }

  async clearStepTimeout(
    transaction: DistributedTransaction,
    step: TransactionStep
  ): Promise<void> {
    throw new Error("Method 'clearStepTimeout' not implemented.")
  }
}
