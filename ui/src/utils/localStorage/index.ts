import { StoreKey } from "./types"

export const getValue = (key: StoreKey) => localStorage.getItem(key) ?? ""

export const setValue = (key: StoreKey, value: string) =>
  localStorage.setItem(key, value)
