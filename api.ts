const TOKEN_KEY = 'protrebot.web.owner-access'
export const USER_SESSION_KEY = 'protrebot-v25-session'

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

export function userSessionToken(): string {
  return localStorage.getItem(USER_SESSION_KEY) || sessionStorage.getItem(USER_SESSION_KEY) || ''
}

export function saveUserSessionToken(token: string, remember: boolean): void {
  localStorage.removeItem(USER_SESSION_KEY)
  sessionStorage.removeItem(USER_SESSION_KEY)
  if (token.trim()) (remember ? localStorage : sessionStorage).setItem(USER_SESSION_KEY, token.trim())
}

export function clearUserSessionToken(): void {
  localStorage.removeItem(USER_SESSION_KEY)
  sessionStorage.removeItem(USER_SESSION_KEY)
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

function isOwnerAccessCheckRequest(input: RequestInfo | URL): boolean {
  return apiRequestPath(input) === `${new URL(API_BASE, window.location.href).pathname}/web/access/check`
}

function isOwnerProtectedApiRequest(input: RequestInfo | URL): boolean {
  const path = apiRequestPath(input)
  if (!path || path === `${new URL(API_BASE, window.location.href).pathname}/health`) return false
  if (path === '/api/v22/bootstrap') return true
  return !['/api/v22', '/api/v24'].some(prefix => path === prefix || path.startsWith(`${prefix}/`))
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
    const userToken = userSessionToken()
    if (userToken && apiRequestPath(input) && !headers.has('Authorization')) {
      headers.set('Authorization', `Bearer ${userToken}`)
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
