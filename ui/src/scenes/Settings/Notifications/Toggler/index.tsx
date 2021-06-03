import React, { useCallback, useContext } from "react"
import { SwitchButton } from "components"
import { LocalStorageContext } from "providers/LocalStorageProvider"
import { StoreKey } from "utils/localStorage/types"

const Toggler = () => {
  const { isNotificationEnabled, updateSettings } = useContext(
    LocalStorageContext,
  )

  const handleSelect = useCallback(
    (value: string) => {
      updateSettings(StoreKey.NOTIFICATION_ENABLED, value)
    },
    [updateSettings],
  )

  return (
    <SwitchButton
      items={[
        { text: "Enable", value: "true" },
        { text: "Disable", value: "false" },
      ]}
      onSelect={handleSelect}
      value={isNotificationEnabled.toString()}
    />
  )
}

export default Toggler
