import { QuestContext } from "providers"
import React, { useContext, useEffect, useState } from "react"
import styled from "styled-components"
import * as QuestDB from "utils/questdb"
import { CopyToClipboard } from "react-copy-to-clipboard"
import { ClipboardCopy } from "@styled-icons/heroicons-outline/ClipboardCopy"
import { SecondaryButton } from "components"
import { formatVersion } from "./services"

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

  useEffect(() => {
    void quest.queryRaw("select build", { limit: "0,1000" }).then((result) => {
      if (result.type === QuestDB.Type.DQL) {
        if (result.count === 1) {
          setBuildVersion(result.dataset[0][0])
        }
      }
    })
  })

  const version = formatVersion(buildVersion)

  return (
    <Wrapper>
      <CopyToClipboard text={buildVersion}>
        <CopyButton title="Copy Build Version">
          <span>{version}</span>
          <ClipboardCopy size="18px" />
        </CopyButton>
      </CopyToClipboard>
    </Wrapper>
  )
}

export default BuildVersion
