import { Observable, of } from "rxjs"
import { fromFetch as rxFromFetch } from "rxjs/fetch"
import { catchError, map, switchMap } from "rxjs/operators"

type ErrorShape = Readonly<{
  error: true
  message: string
}>

type SuccessShape<T> = Readonly<{
  data: T
  error: false
}>

export const fromFetch = <T extends Record<string, any>>(
  uri: string,
  init: RequestInit = {},
): Observable<SuccessShape<T> | ErrorShape> =>
  rxFromFetch(`http://localhost:${BACKEND_PORT}/${uri}`, init).pipe(
    switchMap((response) => {
      if (response.ok) {
        return response.json()
      }

      // Server is returning a status requiring the client to try something else
      return of({
        error: true,
        message: response.statusText,
      })
    }),
    catchError((error: Error) => {
      // Network or other error, handle appropriately
      console.error(error)
      return of({ error: true, message: error.message })
    }),
    map((response: ErrorShape | T) => {
      if (!response.error) {
        return { data: response as T, error: false }
      }

      return response as ErrorShape
    }),
  )
