/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

import React, { forwardRef, Ref, useCallback, useRef, useState } from "react"
import { useDispatch, useSelector } from "react-redux"
import styled from "styled-components"
import { Close } from "@styled-icons/remix-line/Close"

import { SecondaryButton, Text } from "components"
import { actions, selectors } from "store"
import { color, fetchApi } from "utils"

import EmailInput from "./EmailInput"

type Props = Readonly<{
  innerRef: Ref<HTMLDivElement>
}>

const Wrapper = styled.div`
  position: relative;
  display: flex;
  flex-direction: column;
  max-width: 600px;
  padding: 6rem 7rem;
  background: ${color("black")};
  box-shadow: 8px 6px 0 0 ${color("draculaBackground")};
  border-radius: 4px;
`

const Title = styled(Text)``

const Description = styled(Text)`
  margin: 4rem 0;
  line-height: 1.75;
`

const Form = styled.form`
  display: flex;
  max-width: 300px;
  width: 100%;
  align-self: center;
  flex-direction: column;
`

const Submit = styled(SecondaryButton)`
  margin-top: 2rem;
`

const CloseIcon = styled(Close)`
  position: absolute;
  right: 1rem;
  top: 1rem;
  color: ${color("gray2")};

  &:hover {
    cursor: pointer;
  }
`

const Link = styled.a`
  color: ${color("draculaCyan")};

  &:hover {
    color: ${color("draculaCyan")};
    text-decoration: underline;
  }
`

const Error = styled(Text)`
  margin-top: 0.5rem;
`

const EMAIL_INVALID = "Email address is missing or not valid"

const PowerUser = ({ innerRef }: Props) => {
  const [error, setError] = useState("")
  const [dirty, setDirty] = useState(false)
  const formNode = useRef<HTMLFormElement | null>(null)
  const dispatch = useDispatch()
  const config = useSelector(selectors.telemetry.getConfig)

  const handleClose = useCallback(() => {
    dispatch(actions.console.setModalId())
  }, [dispatch])

  const handleInit = useCallback(() => {
    setError("")
    setDirty(true)
  }, [])

  const handleSubmit = useCallback(async () => {
    if (formNode.current == null) {
      return
    }

    const valid = formNode.current.checkValidity()
    setDirty(false)

    if (valid) {
      const formData = new FormData(formNode.current)
      const response = await fetchApi("profile", {
        body: JSON.stringify({
          email: formData.get("email"),
          id: config?.id,
        }),
        method: "POST",
      })

      if (response.error && response.status === 401) {
        setError(EMAIL_INVALID)
      } else {
        setError("")
        dispatch(actions.console.setModalId())
      }
    } else {
      setError(EMAIL_INVALID)
    }
  }, [config, dispatch, formNode])

  const handleLinkClick = useCallback(async () => {
    await fetchApi("profile", {
      body: JSON.stringify({
        id: config?.id,
        slack: true,
      }),
      method: "POST",
    })
    dispatch(actions.console.setModalId())
  }, [config, dispatch])

  return (
    <Wrapper ref={innerRef}>
      <Title color="white" size="hg">
        Let&rsquo;s get in touch!
      </Title>

      <Description color="white" size="lg">
        We&rsquo;d love to get your feedback on features and performance in
        QuestDB. Let us know your thoughts on{" "}
        <Link
          href="https://slack.questdb.io"
          onClick={handleLinkClick}
          rel="noopener noreferrer"
          target="_blank"
        >
          our community slack
        </Link>{" "}
        or share your email below so we can get in contact.
      </Description>

      <Form ref={formNode}>
        <Text color="white" htmlFor="name" size="lg" type="label">
          Email
        </Text>

        <EmailInput dirty={dirty} onInit={handleInit} />

        {error !== "" && (
          <Error color="draculaOrange" size="md">
            {error}
          </Error>
        )}

        <Submit fontSize="lg" onClick={handleSubmit} size="lg">
          Send
        </Submit>
      </Form>
      <CloseIcon onClick={handleClose} size="24px" />
    </Wrapper>
  )
}

const PowerUserWithRef = (
  props: Omit<Props, "innerRef">,
  ref: Ref<HTMLDivElement>,
) => <PowerUser {...props} innerRef={ref} />

export default forwardRef(PowerUserWithRef)
