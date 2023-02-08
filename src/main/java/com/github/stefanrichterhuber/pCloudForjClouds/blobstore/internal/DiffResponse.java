package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.util.Date;
import java.util.List;
import java.util.Objects;

import com.google.gson.annotations.Expose;

/**
 * 
 * 
 * @see https://docs.pcloud.com/methods/general/diff.html
 * @author stefan
 *
 */
public class DiffResponse {
    public static class DiffEntry {
        public static class DiffMetadata {
            @Expose
            private String name;
            @Expose
            private Date created;
            @Expose
            private boolean ismine;
            @Expose
            private boolean thumb;
            @Expose
            private Date modified;
            @Expose
            private String id;
            @Expose
            private boolean isshared;
            @Expose
            private String icon;
            @Expose
            private boolean isfolder;
            @Expose
            private long parentfolderid;
            @Expose
            private long folderid;
            @Expose
            private long fileid;
            @Expose
            private String hash;
            @Expose
            private long size;
            @Expose
            private String contenttype;
            @Expose
            private int category;

            @Override
            public String toString() {
                if (isFolder()) {
                    return String.format("Folder %s with id %d", this.getName(), this.getFolderid());
                } else {
                    return String.format("File %s with id %d", this.getName(), this.getFileid());
                }
            }

            public String getName() {
                return name;
            }

            public Date getCreated() {
                return created;
            }

            public boolean isMine() {
                return ismine;
            }

            public boolean isThumb() {
                return thumb;
            }

            public Date getModified() {
                return modified;
            }

            public String getId() {
                return id;
            }

            public boolean isIsshared() {
                return isshared;
            }

            public String getIcon() {
                return icon;
            }

            public boolean isFolder() {
                return isfolder;
            }

            public long getParentfolderid() {
                return parentfolderid;
            }

            public long getFolderid() {
                return folderid;
            }

            public long getFileid() {
                return fileid;
            }

            public String getHash() {
                return hash;
            }

            public long getSize() {
                return size;
            }

            public String getContenttype() {
                return contenttype;
            }

            public int getCategory() {
                return category;
            }

            @Override
            public int hashCode() {
                return Objects.hash(fileid, folderid, id, isfolder);
            }

            @Override
            public boolean equals(Object obj) {
                if (this == obj)
                    return true;
                if (obj == null)
                    return false;
                if (getClass() != obj.getClass())
                    return false;
                DiffMetadata other = (DiffMetadata) obj;
                return fileid == other.fileid && folderid == other.folderid && Objects.equals(id, other.id)
                        && isfolder == other.isfolder;
            }
        }

        @Expose
        private String event;
        @Expose
        private Date time;
        @Expose
        private int diffid;
        @Expose
        private DiffMetadata metadata;

        public String getEvent() {
            return event;
        }

        public Date getTime() {
            return time;
        }

        public int getDiffid() {
            return diffid;
        }

        public DiffMetadata getMetadata() {
            return metadata;
        }

        @Override
        public String toString() {
            return String.format("%d: %s -> %s", getDiffid(), getEvent(), getMetadata());
        }

        @Override
        public int hashCode() {
            return Objects.hash(diffid, event, metadata, time);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            DiffEntry other = (DiffEntry) obj;
            return diffid == other.diffid && Objects.equals(event, other.event)
                    && Objects.equals(metadata, other.metadata) && Objects.equals(time, other.time);
        }

    }

    @Expose
    private int result;
    @Expose
    private int diffid;
    @Expose
    private List<DiffEntry> entries;

    public int getResult() {
        return result;
    }

    public boolean isOk() {
        return getResult() == 0;
    }

    public int getDiffid() {
        return diffid;
    }

    public List<DiffEntry> getEntries() {
        return entries;
    }
}
