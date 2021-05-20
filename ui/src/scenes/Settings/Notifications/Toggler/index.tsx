import React, { useCallback, useState } from "react"
import { SwitchButton } from "components"
import { getValue, setValue } from "utils/localStorage"

const Toggler = () => {
  const persistedValue = getValue("notification.enabled") ?? "on"
  const [toggled, setToggled] = useState<string>(persistedValue)

  const handleSelect = useCallback((value: string) => {
    setToggled(value)
    setValue("notification.enabled", value)
  }, [])

  return (
    <SwitchButton
      items={[
        { text: "On", value: "on" },
        { text: "Off", value: "off" },
      ]}
      onSelect={handleSelect}
      value={toggled}
    />
  )
}

export default Toggler
