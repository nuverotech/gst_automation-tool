import axios from 'axios';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export interface GSTR2BUploadResponse {
  success: boolean;
  message: string;
  file_path: string;
  filename: string;
}

export interface GSTR2BUpload2BFilesResponse {
  success: boolean;
  message: string;
  file_paths: string[];
  filenames: string[];
  count: number;
}

export interface GSTR2BProcessResponse {
  success: boolean;
  message: string;
  job_id: number;
  task_id?: string;
}

export interface GSTR2BStatusResponse {
  job_id: number;
  status: string;
  created_at: string;
  updated_at?: string;
  completed_at?: string;
  error_message?: string;
  processing_metadata?: any;
  result_file_path?: string;
  purchase_filename: string;
  gstr2b_filenames: string[];
}

export interface GSTR2BJobResponse {
  id: number;
  user_id: number;
  purchase_filename: string;
  gstr2b_filenames: string[];
  status: string;
  created_at: string;
  completed_at?: string;
  error_message?: string;
}

class GSTR2BService {
  private getAuthHeaders() {
    const token = localStorage.getItem('access_token');
    return {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    };
  }

  async uploadPurchaseBook(file: File): Promise<GSTR2BUploadResponse> {
    const formData = new FormData();
    formData.append('file', file);

    const response = await axios.post(
      `${API_BASE_URL}/api/v1/gstr2b/upload-purchase`,
      formData,
      this.getAuthHeaders()
    );

    return response.data;
  }

  async uploadGSTR2BFiles(files: File[]): Promise<GSTR2BUpload2BFilesResponse> {
    const formData = new FormData();
    files.forEach((file) => {
      formData.append('files', file);
    });

    const response = await axios.post(
      `${API_BASE_URL}/api/v1/gstr2b/upload-2b-files`,
      formData,
      this.getAuthHeaders()
      // Don't set Content-Type manually - axios will set it with boundary for FormData
    );

    return response.data;
  }

  async processReconciliation(): Promise<GSTR2BProcessResponse> {
    const response = await axios.post(
      `${API_BASE_URL}/api/v1/gstr2b/process`,
      {},
      this.getAuthHeaders()
    );

    return response.data;
  }

  async getJobStatus(jobId: number): Promise<GSTR2BStatusResponse> {
    const response = await axios.get(
      `${API_BASE_URL}/api/v1/gstr2b/status/${jobId}`,
      this.getAuthHeaders()
    );

    return response.data;
  }

  async downloadReport(jobId: number): Promise<Blob> {
    const response = await axios.get(
      `${API_BASE_URL}/api/v1/gstr2b/download/${jobId}`,
      {
        ...this.getAuthHeaders(),
        responseType: 'blob',
      }
    );

    return response.data;
  }

  async getUserJobs(): Promise<GSTR2BJobResponse[]> {
    const response = await axios.get(
      `${API_BASE_URL}/api/v1/gstr2b/jobs`,
      this.getAuthHeaders()
    );

    return response.data;
  }

  downloadBlob(blob: Blob, filename: string) {
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(url);
  }
}

export default new GSTR2BService();
