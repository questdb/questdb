import $ from "jquery"

$.fn.listManager = function (
  pCreateCallback,
  pCopyToFormCallback,
  pCopyToMemCallback,
  pClearCallback,
) {
  const div = $(this)
  const ulList = div.find(".qdb-list ul")
  const createCallback = pCreateCallback
  const copyToFormCallback = pCopyToFormCallback
  const copyToMemCallback = pCopyToMemCallback
  const clearCallback = pClearCallback
  const map = []
  const btnAdd = div.find(".qdb-list-add-btn")
  const btnDelete = div.find(".qdb-list-delete-btn")
  const divForm = div.find(".qdb-vis-form")
  const divPlaceholder = div.find(".qdb-vis-placeholder")

  let usrUpdateCallaback
  let activeItem

  function updateVisibility() {
    const count = ulList.find("li").length
    if (count === 0) {
      divForm.hide()
      divPlaceholder.show()
    } else {
      divPlaceholder.hide()
      divForm.show()
    }
  }

  function clearUL() {
    const node = ulList[0]
    while (node.firstChild) {
      node.removeChild(node.firstChild)
    }
  }

  function clearMap() {
    for (let k in map) {
      if (map.hasOwnProperty(k)) {
        delete map[k]
      }
    }
  }

  function activeLi() {
    return ulList.find("#" + activeItem)
  }

  function switchTo(evt) {
    if (activeItem) {
      const html = activeLi()[0]
      if (html) {
        if (!copyToMemCallback(map[activeItem])) {
          return
        }
        html.className = ""
      }
    }

    activeItem = evt.target.getAttribute("id")
    evt.target.className = "active"

    const series = map[activeItem]

    if (series) {
      copyToFormCallback(series)
    } else {
      clearCallback()
    }
  }

  function callUsrUpdateCallback(item) {
    if (usrUpdateCallaback) {
      usrUpdateCallaback(item)
    }
  }

  function updateCallback() {
    const html = ulList.find("#" + this.id)
    html.html(this.name)
    callUsrUpdateCallback(this)
  }

  function addItem0(item) {
    item.callback = updateCallback
    const id = item.id
    const name = item.name
    const html = $(`<li id="${id}">${name}</li>`)
    map[id] = item
    html.click(switchTo)
    html.appendTo(ulList)
    return html
  }

  function addItem() {
    let next = ulList.find("li").length + 1
    let item

    do {
      item = createCallback(next++)
    } while (ulList.find("#" + item.id).length > 0)

    item.timestamp = new Date().getTime()
    const html = addItem0(item)
    updateVisibility()
    html.click()

    callUsrUpdateCallback(item)
  }

  function deleteItem() {
    if (activeItem) {
      const html = activeLi()
      delete map[activeItem]
      callUsrUpdateCallback()
      if (html) {
        clearCallback()
        let next = html.next()
        if (next.length === 0) {
          next = html.prev()
        }
        html.remove()
        if (next.length === 0) {
          activeItem = null
        } else {
          next.click()
        }
      }
    }

    updateVisibility()
  }

  function getMap() {
    return map
  }

  function onUpdate(callback) {
    usrUpdateCallaback = callback
  }

  function setMap(items) {
    clearUL()
    clearMap()

    let first

    for (let i = 0, n = items.length; i < n; i++) {
      const html = addItem0(items[i])
      if (!first) {
        first = html
      }
    }

    if (first) {
      first.click()
    }

    updateVisibility()
  }

  btnAdd.click(addItem)
  btnDelete.click(deleteItem)

  updateVisibility()

  return {
    getMap,
    setMap,
    onUpdate,
  }
}
