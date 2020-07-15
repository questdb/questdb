import $ from "jquery"

$.fn.splitter = function (msgBus, pName, pMinTop, pMinBottom) {
  const bus = $(msgBus)
  const div = $(this)
  const busMsgName = "splitter." + pName + ".resize"
  let ghost
  let start
  let end
  let styleMain
  const minTop = pMinTop
  const minBottom = pMinBottom

  function drag(e) {
    e.preventDefault()
    if (
      e.pageY > minTop &&
      e.pageY < window.innerHeight + $(window).scrollTop() - minBottom
    ) {
      end = e.pageY
      ghost[0].style = styleMain + "top: " + e.pageY + "px;"
    }
  }

  function touchDrag(e) {
    e.preventDefault()
    const y = e.originalEvent.touches[0].clientY
    if (
      y > minTop &&
      y < window.innerHeight + $(window).scrollTop() - minBottom
    ) {
      end = y
      ghost[0].style = styleMain + "top: " + y + "px;"
    }
  }

  function endDrag() {
    $(document).off("mousemove", drag)
    $(document).off("mouseup", endDrag)
    ghost[0].style = "display: none"
    div.removeClass("qs-dragging")
    bus.trigger(busMsgName, end - start)
  }

  function beginDrag() {
    const rect = div[0].getBoundingClientRect()
    start = rect.top + $(window).scrollTop()
    styleMain =
      "position: absolute; left: " +
      rect.left +
      "px; width: " +
      rect.width +
      "px; height: " +
      rect.height +
      "px;"
    if (!ghost) {
      ghost = $('<div class="qs-ghost"></div>')
      ghost.appendTo("body")
    }
    ghost[0].style = styleMain + "top: " + start + "px;"
    div.addClass("qs-dragging")
    $(document).mousemove(drag)
    $(document).mouseup(endDrag)
  }

  $(this).mousedown(beginDrag)
  $(this).on("touchstart", beginDrag)
  $(this).on("touchmove", touchDrag)
  $(this).on("touchend", endDrag)
}
