import axios from "axios"

export const baseURL = import.meta.env.VITE_API_BASE_URL || ""

// --- Session ID management ---
// The backend identifies users by a session uid stored in a cookie.
// Cookies can be unreliable for SSE (EventSource) connections through proxies
// and in some cross-origin scenarios. To ensure consistency, we fetch the
// session ID once via a regular request and pass it explicitly on all
// subsequent requests (as a header) and on SSE URLs (as a query param).

let _sessionId = null
let _sessionIdPromise = null

export async function ensureSessionId() {
  if (_sessionId) return _sessionId
  if (_sessionIdPromise) return _sessionIdPromise

  _sessionIdPromise = (async () => {
    try {
      // Use raw fetch to avoid going through the axios interceptor
      const res = await fetch(`${baseURL}/api/sid`, { credentials: 'include' })
      if (res.ok) {
        const data = await res.json()
        _sessionId = data.sid
        return _sessionId
      }
      return null
    } catch (e) {
      return null
    } finally {
      _sessionIdPromise = null
    }
  })()

  return _sessionIdPromise
}

export const api = axios.create({
  baseURL,
  withCredentials: true,
})

// Attach the cached session ID as a header on all outgoing axios requests
api.interceptors.request.use((config) => {
  if (_sessionId) {
    config.headers['X-Session-ID'] = _sessionId
  }
  return config
})

export function useApi() {
  const get = async (url) => {
    await ensureSessionId()
    try {
      const res = await api.get(url)
      return res.data
    } catch (err) {
      throw err.response ? new Error(`HTTP error! status: ${err.response.status}`) : err
    }
  }

  const post = async (url, data) => {
    await ensureSessionId()
    try {
      const res = await api.post(url, data)
      return res.data
    } catch (err) {
      throw err.response ? new Error(`HTTP error! status: ${err.response.status}`) : err
    }
  }

  const upload = async (url, formData) => {
    await ensureSessionId()
    try {
      const res = await api.post(url, formData, {
        headers: { "Content-Type": "multipart/form-data" },
      })
      return res.data
    } catch (err) {
      throw err.response ? new Error(`HTTP error! status: ${err.response.status}`) : err
    }
  }

  return { get, post, upload, api }
}
