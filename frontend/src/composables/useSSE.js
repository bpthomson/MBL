import { ref, onUnmounted } from 'vue'
import { baseURL, ensureSessionId } from './useApi'

export function useSSE(url) {
  const status = ref('Connecting...')
  const messages = ref([])
  const error = ref(null)
  const progress = ref('0%')
  const isDone = ref(false)
  let eventSource = null

  const connect = async (targetUrl) => {
    if (eventSource) eventSource.close()

    // Ensure session ID is fetched and append to URL as a query param,
    // since EventSource connections through proxies may not carry cookies.
    const sid = await ensureSessionId()
    if (sid) {
      const sep = targetUrl.includes('?') ? '&' : '?'
      targetUrl = `${targetUrl}${sep}sid=${encodeURIComponent(sid)}`
    }

    const fullUrl = targetUrl.startsWith('http') ? targetUrl : `${baseURL}${targetUrl}`
    eventSource = new EventSource(fullUrl, { withCredentials: true })
    
    eventSource.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data)
        if (data.error) {
          error.value = data.error
          status.value = 'Error'
          close()
        } else if (data.done) {
          isDone.value = true
          status.value = 'Complete'
          close()
          messages.value.push(data) 
        } else {
          messages.value.push(data)
          if (data.status) status.value = data.status
          if (data.progress) progress.value = data.progress
        }
      } catch (err) {
        console.error("Parse error:", err, e.data)
      }
    }
    
    eventSource.onerror = (e) => {
      error.value = "Connection lost or server error"
      status.value = 'Disconnected'
      close()
    }
  }

  const close = () => {
    if (eventSource) {
      eventSource.close()
      eventSource = null
    }
  }

  onUnmounted(() => {
    close()
  })

  return { status, messages, error, progress, isDone, connect, close }
}
