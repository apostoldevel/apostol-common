/*++

Program name:

  Apostol CRM

Module Name:

  FileCommon.cpp

Notices:

  Module: File Common

Author:

  Copyright (c) Prepodobny Alen

  mailto: alienufo@inbox.ru
  mailto: ufocomp@gmail.com

--*/

//----------------------------------------------------------------------------------------------------------------------

#include "Core.hpp"
#include "BackEnd.hpp"
#include "FileCommon.hpp"
//----------------------------------------------------------------------------------------------------------------------

#define API_BOT_USERNAME "apibot"
#define PG_CONFIG_NAME "helper"

#define QUERY_INDEX_AUTH     0
#define QUERY_INDEX_DATA     1

#define FILE_SERVER_ERROR_MESSAGE "[%s] Error: %s"
//----------------------------------------------------------------------------------------------------------------------

extern "C++" {

namespace Apostol {

    namespace Module {

        //--------------------------------------------------------------------------------------------------------------

        //-- CFileHandler ----------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CFileHandler::CFileHandler(CQueueCollection *ACollection, const CString &Data, COnQueueHandlerEvent && Handler):
                CQueueHandler(ACollection, static_cast<COnQueueHandlerEvent &&> (Handler)) {

            m_Payload = Data;

            m_Session = m_Payload["session"].AsString();
            m_Operation = m_Payload["operation"].AsString();
            m_FileId = m_Payload["id"].AsString();
            m_Type = m_Payload["type"].AsString();
            m_Path = m_Payload["path"].AsString();
            m_Name = m_Payload["name"].AsString();
            m_Hash = m_Payload["hash"].AsString();

            m_pConnection = nullptr;

            m_TimeOutInterval = 30 * 60 * 1000;

            UpdateTimeOut(Now());
        }
        //--------------------------------------------------------------------------------------------------------------

        CFileHandler::~CFileHandler() {
            SetConnection(nullptr);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileHandler::SetConnection(CHTTPServerConnection *AConnection) {
            if (m_pConnection != AConnection) {
                if (AConnection != nullptr) {
                    AConnection->Binding(this);
                    AConnection->TimeOut(INFINITE);
                } else {
                    if (m_pConnection != nullptr) {
                        m_pConnection->TimeOutInterval(5 * 1000);
                        m_pConnection->UpdateTimeOut(Now());
                        m_pConnection->Binding(nullptr);
                        m_pConnection->CloseConnection(true);
                    }
                }

                m_pConnection = AConnection;
            }
        }

        //--------------------------------------------------------------------------------------------------------------

        //-- CFileCommon -----------------------------------------------------------------------------------------------

        //--------------------------------------------------------------------------------------------------------------

        CFileCommon::CFileCommon(CModuleProcess *AProcess, const CString &ModuleName, const CString &SectionName):
                CQueueCollection(Config()->PostgresPollMin()), CApostolModule(AProcess, ModuleName, SectionName) {

            m_Headers.Add("Authorization");

            m_Agent = CString().Format("%s (%s)", GApplication->Title().c_str(), ModuleName.c_str());
            m_Host = CApostolModule::GetIPByHostName(CApostolModule::GetHostName());

            m_TimeOut = 0;
            m_AuthDate = 0;

            m_Client.AllocateEventHandlers(Server());
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
            m_Client.OnException([this](auto &&Sender, auto &&E) { DoCurlException(Sender, E); });
#else
            m_Client.OnException(std::bind(&CFileCommon::DoCurlException, this, _1, _2));
#endif
        }
        //--------------------------------------------------------------------------------------------------------------

        CPQPollQuery *CFileCommon::GetQuery(CPollConnection *AConnection, const CString &ConfName) {
            return CApostolModule::GetQuery(AConnection, PG_CONFIG_NAME);
        }
        //--------------------------------------------------------------------------------------------------------------

        CPQPollQuery *CFileCommon::ExecuteSQL(const CStringList &SQL, CFileHandler *AHandler,
                COnApostolModuleSuccessEvent &&OnSuccess, COnApostolModuleFailEvent &&OnFail) {

            auto OnExecuted = [this, OnSuccess](CPQPollQuery *APollQuery) {
                const auto pHandler = dynamic_cast<CFileHandler *> (APollQuery->Binding());
                const auto pConnection = pHandler->Connection();
                if (pConnection != nullptr && pConnection->Connected()) {
                    OnSuccess(pConnection, APollQuery);
                }
                DeleteHandler(pHandler);
            };

            auto OnException = [this, OnFail](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                const auto pHandler = dynamic_cast<CFileHandler *> (APollQuery->Binding());
                const auto pConnection = pHandler->Connection();
                if (pConnection != nullptr && pConnection->Connected()) {
                    OnFail(pConnection, E);
                }
                DeleteHandler(pHandler);
            };

            return ExecSQL(SQL, AHandler, OnExecuted, OnException);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::Authentication() {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                CStringList SQL;

                try {
                    CPQueryResults pqResults;
                    CApostolModule::QueryToResults(APollQuery, pqResults);

                    const auto &session = pqResults[0].First()["session"];

                    m_Session = pqResults[1].First()["get_session"];

                    m_AuthDate = Now() + static_cast<CDateTime>(24) / HoursPerDay;

                    SignOut(session);
                } catch (Delphi::Exception::Exception &E) {
                    DoError(E);
                }
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                DoError(E);
            };

            const auto &caProviders = Server().Providers();
            const auto &caProvider = caProviders.DefaultValue();

            const auto &clientId = caProvider.ClientId(SERVICE_APPLICATION_NAME);
            const auto &clientSecret = caProvider.Secret(SERVICE_APPLICATION_NAME);

            CStringList SQL;

            api::login(SQL, clientId, clientSecret, m_Agent, m_Host);
            api::get_session(SQL, API_BOT_USERNAME, m_Agent, m_Host);

            try {
                ExecSQL(SQL, nullptr, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::SignOut(const CString &Session) {
            CStringList SQL;

            api::signout(SQL, Session);

            try {
                ExecSQL(SQL);
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::SendFile(CHTTPServerConnection *AConnection, const CString &FileName) {
            if (AConnection != nullptr && AConnection->Connected()) {
                auto &Reply = AConnection->Reply();

                CString sFileExt;
                TCHAR szBuffer[MAX_BUFFER_SIZE + 1] = {0};

                const auto sModified = StrWebTime(FileAge(FileName.c_str()), szBuffer, sizeof(szBuffer));
                if (sModified != nullptr) {
                    AConnection->Reply().AddHeader(_T("Last-Modified"), sModified);
                }

                sFileExt = ExtractFileExt(szBuffer, FileName.c_str());

#if (APOSTOL_USE_SEND_FILE)
    #if (OPENSSL_VERSION_NUMBER >= 0x30000000L) && defined(BIO_get_ktls_send)
                AConnection->SendFileReply(FileName.c_str(), Mapping::ExtToType(sFileExt.c_str()));
    #else
                if (AConnection->IOHandler()->UsedSSL()) {
                    if (Reply.Content.IsEmpty()) {
                        Reply.Content.LoadFromFile(FileName.c_str());
                    }

                    AConnection->SendReply(CHTTPReply::ok, Mapping::ExtToType(sFileExt.c_str()), true);
                } else {
                    AConnection->SendFileReply(FileName.c_str(), Mapping::ExtToType(sFileExt.c_str()));
                }
    #endif
#else
                if (Reply.Content.IsEmpty()) {
                    Reply.Content.LoadFromFile(FileName.c_str());
                }

                AConnection->SendReply(CHTTPReply::ok, Mapping::ExtToType(sFileExt.c_str()), true);
#endif
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DeleteFile(const CString &FileName) {
            if (FileExists(FileName.c_str())) {
                CApplication::DeleteFile(FileName);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoError(const Delphi::Exception::Exception &E) const {
            Log()->Error(APP_LOG_ERR, 0, FILE_SERVER_ERROR_MESSAGE, ModuleName().c_str(), E.what());
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoError(CQueueHandler *AHandler, const CString &Message) {
            Log()->Error(APP_LOG_ERR, 0, FILE_SERVER_ERROR_MESSAGE, ModuleName().c_str(), Message.c_str());
            DeleteHandler(AHandler);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoCurlException(CCURLClient *Sender, const Delphi::Exception::Exception &E) const {
            DoError(E);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DeleteHandler(CQueueHandler *AHandler) {
            if (Assigned(AHandler)) {
                CQueueCollection::DeleteHandler(AHandler);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoFetch(CFileHandler *AHandler) {

            auto OnRequest = [AHandler](CHTTPClient *Sender, CHTTPRequest &Request) {
                if (Assigned(AHandler)) {
                    CHTTPRequest::Prepare(Request, "GET", AHandler->URI().href().c_str());
                }

                DebugRequest(Request);
            };
            //----------------------------------------------------------------------------------------------------------

            auto OnExecute = [this, AHandler](CTCPConnection *Sender) {
                const auto pConnection = dynamic_cast<CHTTPClientConnection *> (Sender);
                const auto &Reply = pConnection->Reply();

                DebugReply(Reply);

                if (Assigned(AHandler)) {
                    const auto pHandlerConnection = AHandler->Connection();

                    if (Reply.Status == CHTTPReply::ok) {
                        try {
                            Reply.Content.SaveToFile(AHandler->AbsoluteName().c_str());
                            SendFile(pHandlerConnection, AHandler->AbsoluteName());
                        } catch (Delphi::Exception::Exception &E) {
                            DoError(E);
                        }

                        DoDone(AHandler, Reply);
                    } else {
                        const CString Message("Not found");

                        if (Server().IndexOfConnection(pHandlerConnection) != -1) {
                            ReplyError(pHandlerConnection, CHTTPReply::not_found, Message);
                        }

                        DoFail(AHandler, Message);
                    }
                }

                return true;
            };
            //----------------------------------------------------------------------------------------------------------

            auto OnException = [this, AHandler](CTCPConnection *Sender, const Delphi::Exception::Exception &E) {
                const auto pConnection = dynamic_cast<CHTTPClientConnection *> (Sender);
                DebugReply(pConnection->Reply());

                if (Assigned(AHandler)) {
                    const auto pHandlerConnection = AHandler->Connection();

                    if (Server().IndexOfConnection(pConnection) != -1) {
                        const CString Message("Not found");
                        ReplyError(pHandlerConnection, CHTTPReply::not_found, Message);
                    }

                    DoFail(AHandler, E.what());
                }

                DoError(E);
            };
            //----------------------------------------------------------------------------------------------------------

            if (AHandler == nullptr)
                return;

            AHandler->Allow(false);

            if (m_TimeOut > 0) {
                AHandler->TimeOut(0);
                AHandler->TimeOutInterval(m_TimeOut * 1000);
                AHandler->UpdateTimeOut(Now());
            }

            auto pClient = GetClient(AHandler->URI().hostname, AHandler->URI().port);
#if defined(_GLIBCXX_RELEASE) && (_GLIBCXX_RELEASE >= 9)
            pClient->OnConnected([this](auto &&Sender) { DoClientConnected(Sender); });
            pClient->OnDisconnected([this](auto &&Sender) { DoClientDisconnected(Sender); });
#else
            pClient->OnConnected(std::bind(&CFileCommon::DoClientConnected, this, _1));
            pClient->OnDisconnected(std::bind(&CFileCommon::DoClientDisconnected, this, _1));
#endif

            pClient->OnRequest(OnRequest);
            pClient->OnExecute(OnExecute);
            pClient->OnException(OnException);

            try {
                pClient->AutoFree(true);
                pClient->Active(true);
            } catch (std::exception &e) {
                DoFail(AHandler, e.what());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoCURL(CFileHandler *AHandler) {

            auto OnDone = [this, AHandler](CCurlFetch *Sender, CURLcode code, const CString &Error) {
                const auto http_code = Sender->GetResponseCode();
                CHTTPReply Reply;

                Reply.Headers.Clear();
                for (int i = 1; i < Sender->Headers().Count(); i++) {
                    const auto &Header = Sender->Headers()[i];
                    Reply.AddHeader(Header.Name(), Header.Value());
                }

                Reply.StatusString = http_code;
                Reply.StatusText = Reply.StatusString;

                Reply.StringToStatus();

                const auto pConnection = AHandler->Connection();

                if (http_code == 200) {
                    DeleteFile(AHandler->AbsoluteName());

                    Reply.Content = Sender->Result();
                    Reply.ContentLength = Reply.Content.Length();

                    Reply.DelHeader("Transfer-Encoding");
                    Reply.DelHeader("Content-Encoding");
                    Reply.DelHeader("Content-Length");

                    Reply.AddHeader("Content-Length", CString::ToString(Reply.ContentLength));

                    Reply.Content.SaveToFile(AHandler->AbsoluteName().c_str());

                    SendFile(pConnection, AHandler->AbsoluteName());

                    DoDone(AHandler, Reply);
                } else {
                    const CString Message("Not found");

                    if (Server().IndexOfConnection(pConnection) != -1) {
                        ReplyError(pConnection, CHTTPReply::not_found, Message);
                    }

                    DoFail(AHandler, Message);
                }
            };
            //----------------------------------------------------------------------------------------------------------

            auto OnFail = [this, AHandler](CCurlFetch *Sender, CURLcode code, const CString &Error) {
                Log()->Warning("[%s] [CURL] %d (%s)", ModuleName().c_str(), (int) code, Error.c_str());
                const auto pConnection = AHandler->Connection();
                if (Server().IndexOfConnection(pConnection) != -1) {
                    ReplyError(pConnection, CHTTPReply::bad_request, Error);
                }
                DoFail(AHandler, Error);
            };
            //----------------------------------------------------------------------------------------------------------

            AHandler->Allow(false);

            if (m_TimeOut > 0) {
                AHandler->TimeOut(0);
                AHandler->TimeOutInterval((m_TimeOut + 10) * 1000);
                AHandler->UpdateTimeOut(Now());
            }

            try {
                m_Client.Get(AHandler->URI(), CHeaders(), OnDone, OnFail);
            } catch (std::exception &e) {
                DoFail(AHandler, e.what());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoDone(CFileHandler *AHandler, const CHTTPReply &Reply) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                const auto pHandler = dynamic_cast<CFileHandler *> (APollQuery->Binding());
                DeleteHandler(pHandler);
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                const auto pHandler = dynamic_cast<CFileHandler *> (APollQuery->Binding());
                DoError(pHandler, E.Message());
            };

            if (AHandler->Done().IsEmpty()) {
                DeleteHandler(AHandler);
                return;
            }

            const auto &caFileId = PQQuoteLiteral(AHandler->FileId());
            const auto &caAbsoluteName = PQQuoteLiteral(AHandler->AbsoluteName());
            const auto &caHash = SHA256(Reply.Content.IsEmpty() ? "" : Reply.Content, true);
            const auto &caContentType = PQQuoteLiteral(Reply.Headers["Content-Type"]);

            CStringList SQL;

            SQL.Add(CString()
                            .MaxFormatSize(256 + caFileId.Size() + caAbsoluteName.Size() + caHash.Size() + caContentType.Size())
                            .Format("SELECT %s(%s::uuid, %s, %d, '%s', %s);",
                                    AHandler->Done().c_str(),
                                    caFileId.c_str(),
                                    caAbsoluteName.c_str(),
                                    Reply.ContentLength,
                                    caHash.c_str(),
                                    caContentType.c_str()
                            ));

            try {
                ExecSQL(SQL, AHandler, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(AHandler, E.Message());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoFail(CFileHandler *AHandler, const CString &Message) {

            auto OnExecuted = [this](CPQPollQuery *APollQuery) {
                const auto pHandler = dynamic_cast<CFileHandler *> (APollQuery->Binding());
                DeleteHandler(pHandler);
            };

            auto OnException = [this](CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
                const auto pHandler = dynamic_cast<CFileHandler *> (APollQuery->Binding());
                DoError(pHandler, E.Message());
            };

            if (AHandler->Fail().IsEmpty()) {
                DeleteHandler(AHandler);
                return;
            }

            const auto &caFileId = PQQuoteLiteral(AHandler->FileId());
            const auto &caMessage = PQQuoteLiteral(Message);

            CStringList SQL;

            SQL.Add(CString()
                            .MaxFormatSize(256 + caFileId.Size() + caMessage.Size())
                            .Format("SELECT %s(%s::uuid, %s);",
                                    AHandler->Fail().c_str(),
                                    caFileId.c_str(),
                                    caMessage.c_str()
                            ));

            try {
                ExecSQL(SQL, AHandler, OnExecuted, OnException);
            } catch (Delphi::Exception::Exception &E) {
                DoError(AHandler, E.Message());
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoPostgresQueryExecuted(CPQPollQuery *APollQuery) {
            try {
                for (int i = 0; i < APollQuery->Count(); i++) {
                    const auto pResult = APollQuery->Results(i);
                    if (pResult->ExecStatus() != PGRES_TUPLES_OK)
                        throw Delphi::Exception::EDBError(pResult->GetErrorMessage());
                }
            } catch (Delphi::Exception::Exception &E) {
                DoError(E);
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoPostgresQueryException(CPQPollQuery *APollQuery, const Delphi::Exception::Exception &E) {
            DoError(E);
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoClientConnected(CObject *Sender) const {
            const auto pConnection = dynamic_cast<CHTTPClientConnection *>(Sender);
            if (Assigned(pConnection)) {
                const auto pSocket = pConnection->Socket();
                if (pSocket != nullptr) {
                    const auto pHandle = pSocket->Binding();
                    if (pHandle != nullptr) {
                        Log()->Notice(_T("[%s] [%s:%d] Client connected."), ModuleName().c_str(), pHandle->PeerIP(), pHandle->PeerPort());
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::DoClientDisconnected(CObject *Sender) const {
            const auto pConnection = dynamic_cast<CHTTPClientConnection *>(Sender);
            if (Assigned(pConnection)) {
                const auto pSocket = pConnection->Socket();
                if (pSocket != nullptr) {
                    const auto pHandle = pSocket->Binding();
                    if (pHandle != nullptr) {
                        Log()->Notice(_T("[%s] [%s:%d] Client disconnected."), ModuleName().c_str(), pHandle->PeerIP(), pHandle->PeerPort());
                    }
                } else {
                    Log()->Notice(_T("[%s] Client disconnected."), ModuleName().c_str());
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::UnloadQueue() {
            const auto index = m_Queue.IndexOf(this);
            if (index != -1) {
                const auto pQueue = m_Queue[index];
                for (int i = 0; i < pQueue->Count(); ++i) {
                    const auto pHandler = static_cast<CFileHandler *> (pQueue->Item(i));
                    if (pHandler != nullptr && pHandler->Allow()) {
                        pHandler->Handler();
                        if (m_Progress >= m_MaxQueue) {
                            break;
                        }
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::CheckTimeOut(CDateTime Now) {
            const auto index = m_Queue.IndexOf(this);
            if (index != -1) {
                const auto pQueue = m_Queue[index];
                for (int i = pQueue->Count() - 1; i >= 0; i--) {
                    const auto pHandler = static_cast<CFileHandler *> (pQueue->Item(i));
                    if (pHandler != nullptr && !pHandler->Allow()) {
                        if ((pHandler->TimeOut() != INFINITE) && (Now >= pHandler->TimeOut())) {
                            DoFail(pHandler, CString().Format("[%s] Killed by timeout: %s", ModuleName().c_str(), pHandler->AbsoluteName().c_str()));
                        }
                    }
                }
            }
        }
        //--------------------------------------------------------------------------------------------------------------

        void CFileCommon::Initialization(CModuleProcess *AProcess) {
            m_Path = Config()->IniFile().ReadString(SectionName().c_str(), "path", "files/");
            m_Type = Config()->IniFile().ReadString(SectionName().c_str(), "type", "curl");
            m_TimeOut = Config()->IniFile().ReadInteger(SectionName().c_str(), "timeout", 60);

            m_Client.TimeOut(m_TimeOut);

            if (!path_separator(m_Path.front())) {
                m_Path = Config()->Prefix() + m_Path;
            }

            if (!path_separator(m_Path.back())) {
                m_Path = m_Path + "/";
            }

            ForceDirectories(m_Path.c_str(), 0755);
        }
        //--------------------------------------------------------------------------------------------------------------

    }
}
}