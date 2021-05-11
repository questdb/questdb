import { QuestContext } from "providers"
import React, { useCallback, useContext, useEffect, useState } from "react"
import styled from "styled-components"
import * as QuestDB from "utils/questdb"
import { CopyToClipboard } from "react-copy-to-clipboard"

const Wrapper = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: flex-start;
  align-items: center;

  & > :not(:last-child) {
    margin-right: 1rem;
  }
`
const Copy = styled.div`
  color: white;
  background: green;
  padding: 2px 5px;
  border-radius: 2px;
`

const BuildVersion = () => {
  const { quest } = useContext(QuestContext)
  const [copied, setCopied] = useState(false)
  const [buildVersion, setBuildVersion] = useState("")

  const handleCopy = useCallback(() => {
    setCopied(true)
  }, [])

  useEffect(() => {
    void quest.queryRaw("select build", { limit: "0,1000" }).then((result) => {
      if (result.type === QuestDB.Type.DQL) {
        if (result.count === 1) {
          setBuildVersion(result.dataset[0][0])
        }
      }
    })
  })

  useEffect(() => {
    let timeout: NodeJS.Timeout
    if (copied) {
      timeout = setTimeout(() => setCopied(false), 5000)
    }
    return () => clearTimeout(timeout)
  }, [copied])

  return (
    <Wrapper>
      {copied && <Copy>Copied!</Copy>}
      <CopyToClipboard onCopy={handleCopy} text={buildVersion}>
        <a>Build version</a>
      </CopyToClipboard>
    </Wrapper>
  )
}

export default BuildVersion
