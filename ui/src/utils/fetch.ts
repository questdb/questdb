type Error = Readonly<{
  error: true
  message: string
  status?: number
}>

type ResponseShape<T> = Readonly<{
  data?: T
  error: false
}>

export const fetchApi = async <T>(
  input: RequestInfo,
  init?: RequestInit,
): Promise<ResponseShape<T> | Error> => {
  try {
    const response = await fetch(input, init)
    const contentType = response.headers.get("content-type")

    if (!response.ok) {
      return {
        error: true,
        message: response.statusText,
        status: response.status,
      }
    }

    if (contentType === "application/json") {
      const data = (await response.json()) as T
      return {
        data,
        error: false,
      }
    }

    return { error: false }
  } catch (ex) {
    const e = ex as { message: string }
    return {
      error: true,
      message: e.message,
    }
  }
}
