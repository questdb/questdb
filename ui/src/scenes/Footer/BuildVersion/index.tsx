import { QuestContext } from "providers"
import React, { useCallback, useContext, useEffect, useState } from "react"
import { useDispatch } from "react-redux"
import styled from "styled-components"
import * as QuestDB from "utils/questdb"
import { ClipboardCopy } from "@styled-icons/heroicons-outline/ClipboardCopy"
import { SecondaryButton, Text } from "components"
import { formatVersion } from "./services"
import { actions } from "store"
import { NotificationType } from "types"

const Wrapper = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: flex-start;
  align-items: center;

  & > :not(:last-child) {
    margin-right: 1rem;
  }
`
const CopyButton = styled(SecondaryButton)`
  & > :not(:last-child) {
    margin-right: 1rem;
  }
`

const BuildVersion = () => {
  const { quest } = useContext(QuestContext)
  const [buildVersion, setBuildVersion] = useState("")
  const dispatch = useDispatch()

  useEffect(() => {
    void quest.queryRaw("select build", { limit: "0,1000" }).then((result) => {
      if (result.type === QuestDB.Type.DQL) {
        if (result.count === 1) {
          setBuildVersion(formatVersion(result.dataset[0][0]))
        }
      }
    })
  })

  const handleCopy = useCallback(() => {
    void navigator.clipboard
      .writeText(buildVersion)
      .then(() => {
        dispatch(
          actions.query.addNotification({
            title: (
              <Text color="draculaForeground">Build version copied !</Text>
            ),
            type: NotificationType.SUCCESS,
          }),
        )
      })
      .catch(() => {
        dispatch(
          actions.query.addNotification({
            title: (
              <Text color="draculaForeground">Build version copy error</Text>
            ),
            type: NotificationType.ERROR,
          }),
        )
      })
  }, [buildVersion, dispatch])

  return (
    <Wrapper>
      <CopyButton onClick={handleCopy} title="Copy Build Version">
        <span>{buildVersion}</span>
        <ClipboardCopy size="18px" />
      </CopyButton>
    </Wrapper>
  )
}

export default BuildVersion
