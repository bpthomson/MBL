export function useApi() {
  const get = async (url) => {
    const res = await fetch(url)
    if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`)
    return res.json()
  }

  const post = async (url, data) => {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data)
    })
    if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`)
    return res.json()
  }

  const upload = async (url, formData) => {
    const res = await fetch(url, {
      method: 'POST',
      body: formData
    })
    if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`)
    return res.json()
  }

  return { get, post, upload }
}
