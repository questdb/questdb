import React, { useCallback, useContext } from "react"
import { ContextMenu, MenuItem } from "components/ContextMenu"
import { QuestContext } from "providers"
import * as QuestDB from "utils/questdb"
import { formatTableSchemaQueryResult } from "./services"

type Props = {
  name: string
}

const ContextualMenu = ({ name }: Props) => {
  const { quest } = useContext(QuestContext)

  const handleCopySchemaToClipboard = useCallback(() => {
    void quest.queryRaw(`table_columns('${name}')`).then((result) => {
      if (result.type === QuestDB.Type.DQL && result.count > 0) {
        const formattedResult = formatTableSchemaQueryResult(name, result)
        void navigator.clipboard.writeText(formattedResult)
      }
    })
  }, [quest, name])

  return (
    <ContextMenu id={name}>
      <MenuItem onClick={handleCopySchemaToClipboard}>
        Copy Schema To Clipboard
      </MenuItem>
      <MenuItem divider />
      <MenuItem>Close</MenuItem>
    </ContextMenu>
  )
}

export default ContextualMenu
