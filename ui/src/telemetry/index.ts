import { API, Table } from "./constants"
import ProcessWorker from "./process.worker.ts"

import type { ConfigShape, WorkerPayloadShape } from "types"
import { fetchApi } from "utils"
import * as QuestDB from "utils/questdb"

type LastUpdatedResponse = Readonly<{ lastUpdated?: string }>

const quest = new QuestDB.Client()

const start = async () => {
  const yearOffset = 24 * 60 * 60 * 1000 * 31 * 12
  const result = await quest.query<ConfigShape>(`${Table.CONFIG} limit -1`)
  let lastUpdated: string | undefined

  // If the user enabled telemetry then we start the webworker
  if (result.type === QuestDB.Type.DQL && result.data[0].enabled) {
    const response = await fetchApi<LastUpdatedResponse>(
      `${API}/last-updated`,
      {
        method: "POST",
        body: JSON.stringify({ id: result.data[0].id }),
      },
    )

    if (response.error || !response.data) {
      return
    }

    lastUpdated = response.data.lastUpdated

    const sendTelemetry = () => {
      const worker: Worker = new ProcessWorker()
      const payload: WorkerPayloadShape = {
        id: result.data[0].id,
        host: window.location.origin,
        lastUpdated:
          lastUpdated ||
          new Date(new Date().getTime() - yearOffset).toISOString(),
      }

      worker.postMessage(payload)
      worker.onmessage = (event: { data?: string }) => {
        lastUpdated = event.data
      }
    }

    sendTelemetry()
    setInterval(sendTelemetry, 36e5)
  }
}

export default start
