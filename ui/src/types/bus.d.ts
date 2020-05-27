// eslint-disable-next-line no-var
declare var bus: {
  on: (event: string, callback: () => void) => void
  trigger: (event: string, payload?: string) => void
}
