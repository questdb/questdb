import { API, Table } from "./constants"

import type { WorkerPayloadShape } from "types"
import { fetchApi } from "utils"
import * as QuestDB from "utils/questdb"

const start = async (payload: WorkerPayloadShape) => {
  const quest = new QuestDB.Client(payload.host)

  const result = await quest.queryRaw(
    `SELECT cast(created as long), event, origin
        FROM ${Table.MAIN}
        WHERE created > '${new Date(payload.lastUpdated).toISOString()}'
    `,
  )

  if (result.type === QuestDB.Type.DQL && result.count > 0) {
    const response = await fetchApi<void>(`${API}/add`, {
      method: "POST",
      body: JSON.stringify({
        columns: result.columns,
        dataset: result.dataset,
        id: payload.id,
      }),
    })

    if (!response.error) {
      const timestamp = result.dataset[result.count - 1][0] as string
      postMessage(new Date(timestamp).toISOString())
    }
  }
}

addEventListener("message", (message: { data: WorkerPayloadShape }) => {
  void start(message.data)
})
