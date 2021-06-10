import React, { useCallback, useContext } from "react"
import { useDispatch } from "react-redux"
import { ContextMenu, MenuItem } from "components/ContextMenu"
import { QuestContext } from "providers"
import * as QuestDB from "utils/questdb"
import { actions } from "store"
import { Text } from "components"
import { NotificationType } from "types"
import { formatTableSchemaQueryResult } from "./services"

type Props = {
  name: string
}

const ContextualMenu = ({ name }: Props) => {
  const { quest } = useContext(QuestContext)
  const dispatch = useDispatch()

  const handleCopySchemaToClipboard = useCallback(() => {
    void quest
      .queryRaw(`table_columns('${name}')`)
      .then((result) => {
        if (result.type === QuestDB.Type.DQL && result.count > 0) {
          const formattedResult = formatTableSchemaQueryResult(name, result)
          void navigator.clipboard.writeText(formattedResult).then(() => {
            dispatch(
              actions.query.addNotification({
                title: (
                  <Text color="draculaForeground">Table Schema copied !</Text>
                ),
                line1: <Text color="draculaForeground">{formattedResult}</Text>,
                type: NotificationType.SUCCESS,
              }),
            )
          })
        }
      })
      .catch(() => {
        dispatch(
          actions.query.addNotification({
            title: (
              <Text color="draculaForeground">Table Schema copy error</Text>
            ),
            type: NotificationType.ERROR,
          }),
        )
      })
  }, [quest, dispatch, name])

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
