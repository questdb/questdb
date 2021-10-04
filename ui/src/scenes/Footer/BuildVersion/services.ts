const buildVersionRegex = /Build Information: (QuestDB [0-9A-Za-z.]*),/

export const formatVersion = (value: string | number | boolean) => {
  const matches = buildVersionRegex.exec(value.toString())

  return matches ? matches[1] : ""
}
