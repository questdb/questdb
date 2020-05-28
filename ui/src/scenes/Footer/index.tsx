import React from "react"
import styled from "styled-components"
import { Github } from "@styled-icons/remix-fill"

import { Link } from "components"

const Copyright = styled.div`
  display: flex;
  padding-left: 1rem;
  align-items: center;
  flex: 1;
`

const Icons = styled.div`
  display: flex;
  padding-right: 1rem;
  align-items: center;
  font-size: 2rem;
`

const Footer = () => (
  <>
    <Copyright>
      Copyright &copy; 2014-{new Date().getFullYear()} QuestDB Ltd.
    </Copyright>
    <Icons>
      <Link
        color="draculaForeground"
        href="https://github.com/questdb/questdb"
        rel="noreferrer"
        target="_blank"
      >
        <Github size="22px" />
      </Link>
    </Icons>
  </>
)

export default Footer
