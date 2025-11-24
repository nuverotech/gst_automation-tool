export interface UploadResponse {
  id: number
  filename: string
  original_filename: string
  status: ProcessingStatus
  task_id: string | null
  processed_file_path: string | null
  error_message: string | null
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
