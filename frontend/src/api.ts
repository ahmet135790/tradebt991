const TOKEN_KEY = 'protrebot.web.owner-access'

function normalizedApiBase(value: string | undefined): string {
  const base = (value || 'http://127.0.0.1:8000').trim().replace(/\/+$/, '')
  return base.endsWith('/api') ? base : `${base}/api`
}

export const API_BASE = normalizedApiBase(import.meta.env.VITE_API_URL)

const originalFetch = window.fetch.bind(window)
let installed = false

export function ownerAccessToken(): string {
  return sessionStorage.getItem(TOKEN_KEY) || ''
}

export function saveOwnerAccessToken(token: string): void {
  sessionStorage.setItem(TOKEN_KEY, token.trim())
}

export function clearOwnerAccessToken(): void {
  sessionStorage.removeItem(TOKEN_KEY)
}

function apiRequestPath(input: RequestInfo | URL): string | null {
  try {
    const target = new URL(input instanceof Request ? input.url : String(input), window.location.href)
    const api = new URL(API_BASE, window.location.href)
    return target.origin === api.origin && (target.pathname === api.pathname || target.pathname.startsWith(`${api.pathname}/`)) ? target.pathname : null
  } catch {
    return null
  }
}

function isOwnerProtectedApiRequest(input: RequestInfo | URL): boolean {
  const path = apiRequestPath(input)
  if (!path) return false
  const apiPath = new URL(API_BASE, window.location.href).pathname
  if (path === `${apiPath}/health`) return false
  if (path === `${apiPath}/web/access/check`) return true
  if (path.startsWith(`${apiPath}/v21/`) || path.startsWith(`${apiPath}/v25/`) || path.startsWith(`${apiPath}/v27/`)) return true
  if (path.startsWith(`${apiPath}/v22/admin/`)) return true
  if (path.startsWith(`${apiPath}/v22/customers`) || path.startsWith(`${apiPath}/v22/subscriptions/activate-demo`) || path.startsWith(`${apiPath}/v22/licenses/`) || path.startsWith(`${apiPath}/v22/plans/`)) return true
  if (path.startsWith(`${apiPath}/v24/overview`) || path.startsWith(`${apiPath}/v24/settings`) || path.startsWith(`${apiPath}/v24/leads`) || path.startsWith(`${apiPath}/v24/support/`)) return true
  if (path.startsWith(`${apiPath}/exchange-connections/test`) || path.startsWith(`${apiPath}/exchange-connections/save`) || path.startsWith(`${apiPath}/exchange-connections/activate`) || path.startsWith(`${apiPath}/exchange-connections/deactivate`) || path.startsWith(`${apiPath}/exchange-connections/credentials`)) return true
  if (path.startsWith(`${apiPath}/v22/`) || path.startsWith(`${apiPath}/v24/`)) return false
  return path.startsWith(`${apiPath}/`)
}

export function installAuthorizedFetch(): void {
  if (installed) return
  installed = true
  window.fetch = (input: RequestInfo | URL, init: RequestInit = {}) => {
    const headers = new Headers(input instanceof Request ? input.headers : undefined)
    new Headers(init.headers).forEach((value, key) => headers.set(key, value))
    const token = ownerAccessToken()
    if (token && isOwnerProtectedApiRequest(input) && !headers.has('X-ProTreBot-Owner')) {
      headers.set('X-ProTreBot-Owner', token)
    }
    return originalFetch(input, {...init, headers})
  }
}

export async function verifyOwnerAccess(token: string): Promise<{authorized: boolean}> {
  const response = await originalFetch(`${API_BASE}/web/access/check`, {
    headers: {'X-ProTreBot-Owner': token.trim()},
  })
  const payload = await response.json().catch(() => null) as {authorized?: boolean;detail?: string}|null
  if (!response.ok || !payload?.authorized) {
    throw new Error(payload?.detail || 'Yönetici erişimi doğrulanamadı.')
  }
  return {authorized: true}
}
