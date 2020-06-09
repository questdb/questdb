import { differenceInMilliseconds } from "date-fns"
import { QueryAction, QueryAT, QueryStateShape } from "types"

export const initialState: QueryStateShape = {
  notifications: [],
  running: false,
}

const query = (state = initialState, action: QueryAction): QueryStateShape => {
  switch (action.type) {
    case QueryAT.ADD_NOTIFICATION: {
      const notifications = [action.payload, ...state.notifications]

      if (notifications.length > 8) {
        notifications.pop()
      }

      return {
        ...state,
        notifications,
      }
    }

    case QueryAT.CLEANUP_NOTIFICATIONS: {
      const notifications = state.notifications.filter(
        ({ createdAt }) =>
          differenceInMilliseconds(new Date(), createdAt) < 15e3,
      )

      if (notifications.length === state.notifications.length) {
        return state
      }

      return {
        ...state,
        notifications,
      }
    }

    case QueryAT.REMOVE_NOTIFICATION: {
      return {
        ...state,
        notifications: state.notifications.filter(
          ({ createdAt }) => createdAt !== action.payload,
        ),
      }
    }

    case QueryAT.STOP_RUNNING: {
      return {
        ...state,
        running: false,
      }
    }

    case QueryAT.TOGGLE_RUNNING: {
      return {
        ...state,
        running: !state.running,
      }
    }

    default:
      return state
  }
}

export default query
