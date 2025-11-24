'use client'

import { useEffect, useState } from 'react'
import { useAuth } from '@/context/AuthContext'
import { useRouter } from 'next/navigation'
import FileUpload from '@/components/FileUpload'
import ProcessingStatus from '@/components/ProcessingStatus'
import DownloadResults from '@/components/DownloadResults'
import ErrorDisplay from '@/components/ErrorDisplay'
import FileHistory from '@/components/FileHistory'
import { LogOut, User, Upload as UploadIcon, History } from 'lucide-react'

export default function DashboardPage() {
  const { user, logout, loading: authLoading } = useAuth()
  const router = useRouter()
  const [uploadId, setUploadId] = useState<number | null>(null)
  const [status, setStatus] = useState<string>('')
  const [error, setError] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState<'upload' | 'history'>('upload')

  useEffect(() => {
    if (!authLoading && !user) {
      router.push('/login')
    }
  }, [user, authLoading, router])

  if (authLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary-600"></div>
      </div>
    )
  }

  if (!user) {
    return null
  }

  const handleUploadSuccess = (id: number) => {
    setUploadId(id)
    setStatus('processing')
    setError(null)
  }

  const handleUploadError = (errorMessage: string) => {
    setError(errorMessage)
    setUploadId(null)
    setStatus('')
  }

  const handleStatusUpdate = (newStatus: string) => {
    setStatus(newStatus)
  }

  const handleReset = () => {
    setUploadId(null)
    setStatus('')
    setError(null)
    setActiveTab('history')
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-50 to-indigo-100">
      {/* Header */}
      <header className="bg-white shadow-sm">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex justify-between items-center">
            <div>
              <h1 className="text-2xl font-bold text-gray-900">GST Automation Tool</h1>
              <p className="text-sm text-gray-600">Welcome back, {user.full_name || user.username}!</p>
            </div>
            <div className="flex items-center space-x-4">
              <div className="flex items-center space-x-2 text-sm text-gray-600">
                <User className="w-4 h-4" />
                <span>{user.email}</span>
              </div>
              <button
                onClick={logout}
                className="flex items-center space-x-2 px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors"
              >
                <LogOut className="w-4 h-4" />
                <span>Logout</span>
              </button>
            </div>
          </div>
        </div>
      </header>

      {/* Tabs */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pt-8">
        <div className="border-b border-gray-200">
          <nav className="-mb-px flex space-x-8">
            <button
              onClick={() => setActiveTab('upload')}
              className={`${
                activeTab === 'upload'
                  ? 'border-primary-500 text-primary-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              } whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm flex items-center space-x-2`}
            >
              <UploadIcon className="w-5 h-5" />
              <span>Upload File</span>
            </button>
            <button
              onClick={() => setActiveTab('history')}
              className={`${
                activeTab === 'history'
                  ? 'border-primary-500 text-primary-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              } whitespace-nowrap py-4 px-1 border-b-2 font-medium text-sm flex items-center space-x-2`}
            >
              <History className="w-5 h-5" />
              <span>File History</span>
            </button>
          </nav>
        </div>
      </div>

      {/* Content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {activeTab === 'upload' ? (
          <div className="bg-white rounded-2xl shadow-xl p-8">
            {error && <ErrorDisplay error={error} onDismiss={() => setError(null)} />}

            {!uploadId && !error && (
              <FileUpload onUploadSuccess={handleUploadSuccess} onUploadError={handleUploadError} />
            )}

            {uploadId && status === 'processing' && (
              <ProcessingStatus uploadId={uploadId} onStatusUpdate={handleStatusUpdate} />
            )}

            {uploadId && status === 'completed' && (
              <DownloadResults uploadId={uploadId} onReset={handleReset} />
            )}

            {uploadId && status === 'failed' && (
              <div className="text-center">
                <ErrorDisplay
                  error="Processing failed. Please try again with a different file."
                  onDismiss={handleReset}
                />
                <button
                  onClick={handleReset}
                  className="mt-4 px-6 py-3 bg-primary-600 text-white rounded-lg hover:bg-primary-700 transition-colors"
                >
                  Upload Another File
                </button>
              </div>
            )}
          </div>
        ) : (
          <FileHistory />
        )}
      </div>
    </div>
  )
}
