export const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

export const API_ENDPOINTS = {
  UPLOAD: `${API_BASE_URL}/api/v1/upload`,
  STATUS: (id: number) => `${API_BASE_URL}/api/v1/status/${id}`,
  DOWNLOAD: (id: number) => `${API_BASE_URL}/api/v1/download/${id}`,
  HEALTH: `${API_BASE_URL}/health`,
}

export const MAX_FILE_SIZE = 50 * 1024 * 1024 // 50MB
export const ALLOWED_FILE_TYPES = [
  'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', // .xlsx
  'application/vnd.ms-excel', // .xls
  'application/vnd.ms-excel.sheet.binary.macroenabled.12', // .xlsb
  'text/csv', // .csv
]

export const POLL_INTERVAL = 2000 // 2 seconds
