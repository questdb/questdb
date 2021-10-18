/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
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

import * as cypress from "cypress"

// Inspired by
// https://docs.cypress.io/guides/references/best-practices.html

// Shortcut for cy.getByCustomData('test')
Cypress.Commands.add("getByDataTest", (selector) => {
  return cy.get(`[data-test="${selector}"]`)
})

// Shortcut for cy.getByCustomData('test*')
Cypress.Commands.add("getByDataTestLike", (selector) => {
  return cy.get(`[data-test*="${selector}"]`)
})

Cypress.Commands.add("getByCustomData", (data, selector) => {
  return cy.get(`[data-${data}="${selector}"]`)
})

Cypress.Commands.add("getSqlEditor", () => {
  return cy.get("#questdb-sql-editor")
})

Cypress.Commands.add("getSqlEditorValue", () => {
  return cy.get("#questdb-sql-editor").find("textarea").invoke("val")
})

Cypress.Commands.add("getGrid", () => {
  return cy.get("#grid")
})

Cypress.Commands.add("getGridHeader", () => {
  return cy.get("#grid .qg-header")
})

Cypress.Commands.add("getGridHeaderRow", () => {
  return cy.get("#grid .qg-header-row")
})

Cypress.Commands.add("getGridRow", (active) => {
  return cy.get(`#grid .qg-r${active ? "-active" : ""}`)
})

Cypress.Commands.add("getGridColumn", () => {
  return cy.get("#grid .qg-c")
})

Cypress.Commands.add("getGridViewport", () => {
  return cy.get("#grid .qg-viewport")
})

Cypress.Commands.add("execQuery", (query) => {
  return cy.request("GET", `/exec?query=${query}`)
})
