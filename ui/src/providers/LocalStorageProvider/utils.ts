export const parseBoolean = (value: string, defaultValue: boolean): boolean =>
  value ? value === "true" : defaultValue

export const parseInteger = (value: string, defaultValue: number): number =>
  isNaN(parseInt(value)) ? defaultValue : parseInt(value)
