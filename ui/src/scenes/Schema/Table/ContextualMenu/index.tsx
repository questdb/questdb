import React, { useCallback, useContext } from "react"
import { useDispatch } from "react-redux"
import { ContextMenu, MenuItem } from "components/ContextMenu"
import { QuestContext } from "providers"
import * as QuestDB from "utils/questdb"
import { actions } from "store"
import { Text } from "components"
import { NotificationType } from "types"

type Props = {
  name: string
}

const ContextualMenu = ({ name }: Props) => {
  const { quest } = useContext(QuestContext)
  const dispatch = useDispatch()

  const handleCopySchemaToClipboard = useCallback(() => {
    void quest.queryRaw("select build", { limit: "0,1000" }).then((result) => {
      if (result.type === QuestDB.Type.DQL) {
        if (result.count === 1) {
          void navigator.clipboard.writeText(result.dataset[0][0]).then(() => {
            dispatch(
              actions.query.addNotification({
                title: (
                  <Text color="draculaForeground">DB Schema copied !</Text>
                ),
                type: NotificationType.SUCCESS,
              }),
            )
          })
        } else {
          dispatch(
            actions.query.addNotification({
              title: (
                <Text color="draculaForeground">DB Schema copy error</Text>
              ),
              type: NotificationType.ERROR,
            }),
          )
        }
      }
    })
  }, [quest, dispatch])

  return (
    <ContextMenu id={name}>
      <MenuItem data={{ foo: "bar" }}>Query</MenuItem>
      <MenuItem data={{ foo: "bar" }} onClick={handleCopySchemaToClipboard}>
        Copy Schema To Clipboard
      </MenuItem>
      <MenuItem divider />
      <MenuItem data={{ foo: "bar" }}>Close</MenuItem>
    </ContextMenu>
  )
}

export default ContextualMenu
