import axios from 'axios'
import { API_ENDPOINTS } from '@/utils/constants'
import { 
  ApiResponse, 
  UploadResponse, 
  StatusResponse, 
  User,
  LoginRequest,
  SignupRequest,
  AuthResponse
} from '@/types'

const api = axios.create({
  baseURL: process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000',
  headers: {
    'Content-Type': 'application/json',
  },
})

// Add token to requests if available
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('access_token')
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

// Auth endpoints
export const signup = async (data: SignupRequest): Promise<User> => {
  const response = await api.post('/api/v1/auth/signup', data)
  return response.data
}

export const login = async (data: LoginRequest): Promise<AuthResponse> => {
  const response = await api.post('/api/v1/auth/login', data)
  return response.data
}

export const getCurrentUser = async (): Promise<User> => {
  const response = await api.get('/api/v1/user/me')
  return response.data
}

export const getUserUploads = async (): Promise<UploadResponse[]> => {
  const response = await api.get('/api/v1/user/uploads')
  return response.data
}

// File upload endpoints
export const uploadFile = async (file: File): Promise<ApiResponse<UploadResponse>> => {
  const formData = new FormData()
  formData.append('file', file)

  const response = await api.post(API_ENDPOINTS.UPLOAD, formData, {
    headers: {
      'Content-Type': 'multipart/form-data',
    },
  })

  return response.data
}

export const getStatus = async (uploadId: number): Promise<ApiResponse<StatusResponse>> => {
  const response = await api.get(API_ENDPOINTS.STATUS(uploadId))
  return response.data
}

export const downloadFile = async (uploadId: number): Promise<Blob> => {
  const response = await api.get(API_ENDPOINTS.DOWNLOAD(uploadId), {
    responseType: 'blob',
  })
  return response.data
}

export const checkHealth = async (): Promise<any> => {
  const response = await api.get(API_ENDPOINTS.HEALTH)
  return response.data
}

export default api
