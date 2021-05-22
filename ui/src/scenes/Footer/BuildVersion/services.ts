const buildVersionRegex = /Build Information: (QuestDB [0-9A-Za-z.]*),/

export const formatVersion = (value: string) => {
  const matches = buildVersionRegex.exec(value)

  return matches ? matches[1] : ""
}
