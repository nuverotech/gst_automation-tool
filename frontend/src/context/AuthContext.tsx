'use client'

import { createContext, useContext, useState, useEffect, ReactNode } from 'react'
import { User, LoginRequest, SignupRequest } from '@/types'
import { login as loginApi, signup as signupApi, getCurrentUser } from '@/services/api'
import { useRouter } from 'next/navigation'

interface AuthContextType {
  user: User | null
  loading: boolean
  login: (data: LoginRequest) => Promise<void>
  signup: (data: SignupRequest) => Promise<void>
  logout: () => void
  isAuthenticated: boolean
}

const AuthContext = createContext<AuthContextType | undefined>(undefined)

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null)
  const [loading, setLoading] = useState(true)
  const router = useRouter()

  useEffect(() => {
    checkAuth()
  }, [])

  const checkAuth = async () => {
    const token = localStorage.getItem('access_token')
    if (token) {
      try {
        const userData = await getCurrentUser()
        setUser(userData)
      } catch (error) {
        localStorage.removeItem('access_token')
      }
    }
    setLoading(false)
  }

  const login = async (data: LoginRequest) => {
    try {
      const response = await loginApi(data)
      localStorage.setItem('access_token', response.access_token)
      const userData = await getCurrentUser()
      setUser(userData)
      router.push('/select-gst-type')
    } catch (error: any) {
      throw new Error(error.response?.data?.detail || 'Login failed')
    }
  }

  const signup = async (data: SignupRequest) => {
    try {
      await signupApi(data)
      // Auto login after signup
      await login({ username: data.username, password: data.password })
    } catch (error: any) {
      throw new Error(error.response?.data?.detail || 'Signup failed')
    }
  }

  const logout = () => {
    localStorage.removeItem('access_token')
    setUser(null)
    router.push('/login')
  }

  return (
    <AuthContext.Provider
      value={{
        user,
        loading,
        login,
        signup,
        logout,
        isAuthenticated: !!user,
      }}
    >
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider')
  }
  return context
}
