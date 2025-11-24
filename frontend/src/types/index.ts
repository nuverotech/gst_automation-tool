export interface UploadResponse {
  id: number
  filename: string
  original_filename: string
  file_path: string
  file_size: number
  status: ProcessingStatus
  task_id: string | null
  processed_file_path: string | null
  error_message: string | null
  processing_metadata: string | null
  created_at: string
  updated_at: string | null
  completed_at: string | null
}

export interface ApiResponse<T> {
  success: boolean
  message: string
  data: T | null
  error: string | null
}

export interface StatusResponse {
  id: number
  status: ProcessingStatus
  task_id: string | null
  processed_file_path: string | null
  error_message: string | null
  progress: number
}

export enum ProcessingStatus {
  PENDING = 'pending',
  PROCESSING = 'processing',
  COMPLETED = 'completed',
  FAILED = 'failed',
}

// Auth types
export interface User {
  id: number
  email: string
  username: string
  full_name: string | null
  is_active: boolean
  is_verified: boolean
  created_at: string
  last_login: string | null
}

export interface LoginRequest {
  username: string
  password: string
}

export interface SignupRequest {
  email: string
  username: string
  password: string
  full_name?: string
}

export interface AuthResponse {
  access_token: string
  token_type: string
}
