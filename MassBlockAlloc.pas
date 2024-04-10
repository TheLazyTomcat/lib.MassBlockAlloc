{-------------------------------------------------------------------------------

  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.

-------------------------------------------------------------------------------}
{===============================================================================

  MassBlockAlloc

    <todo>

  Version <todo> (<todo>)

  Last change <todo>

  ©<todo> František Milt

  Contacts:
    František Milt: frantisek.milt@gmail.com

  Support:
    If you find this code useful, please consider supporting its author(s) by
    making a small donation using the following link(s):

      https://www.paypal.me/FMilt

  Changelog:
    For detailed changelog and history please refer to this git repository:

      github.com/TheLazyTomcat/Lib.MassBlockAlloc

  Dependencies:
    <todo>

===============================================================================}
unit MassBlockAlloc;
{
  MassBlockAlloc_UseAuxExceptions
  All_UseAuxExceptions

  If you want library-specific exceptions to be based on more advanced classes
  provided by AuxExceptions library instead of basic Exception class, and don't
  want to or cannot change code in this unit, you can define global symbol
  MassBlockAlloc_UseAuxExceptions or All_UseAuxExceptions to do so.

    NOTE - if you globally define symbol All_UseAuxExceptions, then exception
           classes in all units that support his feature will be rebased, not
           only classes from this unit.
}
{$IF Defined(MassBlockAlloc_UseAuxExceptions) or Defined(All_UseAuxExceptions)}
  {$DEFINE UseAuxExceptions}
{$IFEND}

{$IF defined(CPU64) or defined(CPU64BITS)}
  {$DEFINE CPU64bit}
{$ELSEIF defined(CPU16)}
  {$MESSAGE FATAL '16bit CPU not supported'}
{$ELSE}
  {$DEFINE CPU32bit}
{$IFEND}

{$IF Defined(WINDOWS) or Defined(MSWINDOWS)}
  {$DEFINE Windows}
{$ELSEIF Defined(LINUX) and Defined(FPC)}
  {$DEFINE Linux}
{$ELSE}
  {$MESSAGE FATAL 'Unsupported operating system.'}
{$IFEND}

{$IFDEF FPC}
  {$MODE ObjFPC}
  {$MODESWITCH DuplicateLocals+}
  {$DEFINE FPC_DisableWarns}
  {$MACRO ON}
{$ENDIF}
{$H+}

//------------------------------------------------------------------------------
{$UNDEF AllowLargeSegments}
{$IFDEF MassBlockAlloc_AllowLargeSegments_On}
  {$DEFINE AllowLargeSegments}
{$ENDIF}

interface

uses
  SysUtils, Classes, SyncObjs,
  AuxTypes, AuxClasses, BitVector, BitOps
  {$IFDEF UseAuxExceptions}, AuxExceptions{$ENDIF};

{===============================================================================
    Public constants
===============================================================================}
const
  OneKiB = TMemSize(1024);          // one kibibyte (2^10, kilobyte for you oldschools :P)
  OneMiB = TMemSize(1024 * OneKiB); // one mebibyte (2^20, megabyte)
  OneGiB = TMemSize(1024 * OneMiB); // one gibibyte (2^30, gigabyte)

{===============================================================================
    Library-specific exception
===============================================================================}
type
  EMBAException = class({$IFDEF UseAuxExceptions}EAEGeneralException{$ELSE}Exception{$ENDIF});

  EMBASystemError      = class(EMBAException);
  EMBAInvalidValue     = class(EMBAException);
  EMBAInvalidState     = class(EMBAException);
  EMBAInvalidAddress   = class(EMBAException);
  EMBAInvalidAction    = class(EMBAException);
  EMBAIndexOutOfBounds = class(EMBAException);
  EMBAOutOfResources   = class(EMBAException);

{===============================================================================
--------------------------------------------------------------------------------
                                   TMBASegment
--------------------------------------------------------------------------------
===============================================================================}
type
  TMBAPointerArray = array of Pointer;

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
type
  TMBASegmentSizingStyle = (szMinCount,szMinSize,szExactSize);

  TMBASegmentSettings = record
    FailOnUnfreed:  Boolean;
    MapInSegment:   Boolean;
    BlockSize:      TMemSize;
    Alignment:      TMemoryAlignment;
    case SizingStyle: TMBASegmentSizingStyle of
      szMinCount:   (BlockCount:  Integer);
      szMinSize,
      szExactSize:  (MemorySize:  TMemSize);
  end;

{===============================================================================
    TMBASegment - class declaration
===============================================================================}
type
  TMBASegment = class(TCustomListObject)
  protected
    fSettings:          TMBASegmentSettings;
    fReservedBlockSize: TMemSize;
    fBlockCount:        Integer;
    fMemorySize:        TMemSize;
    fMemory:            Pointer;
    fAllocationMap:     TBitVectorStatic;
    fLowAddress:        Pointer;  // address of first block
    fHighAddress:       Pointer;  // address BEHIND the last block (reserved size)
    // getters, setters
    Function GetIsAllocated(Index: Integer): Boolean; virtual;
    Function GetBlockAddress(Index: Integer): Pointer; virtual;
    // list methods
    Function GetCapacity: Integer; override;
    procedure SetCapacity(Value: Integer); override;
    Function GetCount: Integer; override;
    procedure SetCount(Value: Integer); override;
    // init/final
    Function CalculateMemorySize: TMemSize; virtual;
    Function CalculateBlockCount: Integer; virtual;
    procedure Initialize(const Settings: TMBASegmentSettings); virtual;
    procedure Finalize; virtual;
    // auxiliary methods and utilities
    Function FindSpaceForBuffer(BufferSize: TMemSize; out RequiredBlockCount: Integer): Integer; virtual;
  public
  {
    MemoryPageSize

    Returns size of memory page (in bytes) as indicated by operating system for
    the calling program.
  }
    class Function MemoryPageSize: TMemSize; virtual;
    constructor Create(const Settings: TMBASegmentSettings);
    destructor Destroy; override;
    Function LowIndex: Integer; override;
    Function HighIndex: Integer; override;
  {
    BufferBlockCount

    Returns number of blocks withing this segment needed to allocate a buffer
    of given size (note that the full RESERVED block size is taken into account
    in this calculation).

      NOTE - it might return a number larger than BlockCount, this is not
             checked as this function is considered to be only informative.
  }
    Function BufferBlockCount(BufferSize: TMemSize): Integer; overload; virtual;
    // address checks
  {
    AddressOwned

    Returns true when the given address is within any present block (points to
    a byte that is part of the block), false otherwise.

    When strict is true, then the address must be within its indicated size
    (Settings.BlockSize), when strict is false then it can lay anywhere within
    reserved block size (ie. it can point into block padding).
  }
    Function AddressOwned(Address: Pointer; Strict: Boolean = False): Boolean; virtual;
  {
    AddressIndexOf

    Returns index of block to which the given address points.

    If strict is true, then only indicated block size is observed, otherwise
    the address can be anywhere within reserved memory of the block.

    When the address does not point to any present block (or points to padding
    when Strict is true), then a negative value is returned.
  }
    Function AddressIndexOf(Address: Pointer; Strict: Boolean = False): Integer; virtual;
  {
    BlockOwned

    Returns true when the given address points to any present block (its
    starting address), false otherwise.
  }
    Function BlockOwned(Block: Pointer): Boolean; virtual;
  {
    BlockIndexOf

    Returns index of block of the given starting address. If the address does
    not point to any block, then a negative value is returned.
  }
    Function BlockIndexOf(Block: Pointer): Integer; virtual;
    // (de)allocation methods
  {
    AllocateBlock

    Selects first unallocated block, marks it as allocated and sets Block param
    to its address. If no block can be allocated, then an exception of class
    EMBAOutOfResources is raised.

    Can also raise an EMBAInvalidState exception if internal data are somehow
    damaged.

    When init memory is set to true, then the block memory is cleared (filled
    with zeroes), otherwise its content is completely undefined and might
    contain content from when it was previously allocated. 
  }
    procedure AllocateBlock(out Block: Pointer; InitMemory: Boolean); virtual;
  {
    FreeBlock

    Marks given block as not allocated and sets Block parameter to nil.

    If given pointer does not point to a block withing this segment, then an
    exception of class EMBAInvalidAddress is raised.

    If freeing buffer that is not allocated, then an EMBAInvalidState exception
    is raised.
  }
    procedure FreeBlock(var Block: Pointer); virtual;
  {
    CanAllocateBuffer

    Returns true if this segmant can allocate buffer of given size, false
    otherwise.
  }
    Function CanAllocateBuffer(BufferSize: TMemSize): Boolean; virtual;
  {
    TryAllocateBuffer

    Tries to allocate buffer of given size. True is returned when it succeeds,
    false otherwise - in which case nothing is allocated and value of Buffer is
    undefined.
  }
    Function TryAllocateBuffer(out Buffer: Pointer; BufferSize: TMemSize; InitMemory: Boolean): Boolean; virtual;
  {
    AllocateBuffer

    Allocates buffer of given size.

    Buffer size must be larger than zero, otherwise an EMBAInvalidValue
    exception is raised.

    If it cannot be allocated (eg. because there is not enough contiguous free
    blocks), then EMBAOutOfResources exception is raised.
  }
    procedure AllocateBuffer(out Buffer: Pointer; BufferSize: TMemSize; InitMemory: Boolean); virtual;
  {
    FreeBuffer

    Deallocates all constituent blocks of given buffer and sets parameter Buffer
    to nil.

    If the Buffer does not point to any block within this segment, then an
    EMBAInvalidAddress exception is raised.

    BufferSize must be larger than zero, otherwise an EMBAInvalidValue exception
    is raised.

    If the buffer size is not a valid number (eg. it is not the same number as
    was used during allocation), then an EMBAInvalidValue or EMBAInvalidState
    exception can be raised.
  }
    procedure FreeBuffer(var Buffer: Pointer; BufferSize: TMemSize); virtual;
    {$message 'todo'}
    //procedure BurstAllocate(out Blocks: TMBAPointerArray); virtual;
    // informative methods
    Function IsFull: Boolean; virtual;
    Function IsEmpty: Boolean; virtual;
    Function AllocatedBlockCount: Integer; virtual;
    Function FreeBlockCount: Integer; virtual;
    // memory statistics
    Function ReservedMemory: TMemSize; virtual;   // memory reserved for all blocks
    Function BlocksMemory: TMemSize; virtual;     // memory of all blocks (excluding padding, so only BlockCount * BlockSize)
    Function AllocatedMemory: TMemSize; virtual;  // memory of allocated blocks (excluding padding)
    Function WastedMemory: TMemSize; virtual;     // all padding (including individual blocks)
    Function MemoryEfficiency: Double; virtual;   // (MemorySize - WastedMemory) / MemorySize (ignores unused space of buffers/vectors)
    Function MemoryUtilization: Double; virtual;  // AllocatedMemory / BlocksMemory (ignores unused space of buffers/vectors)
    // properties
    property Capacity: Integer read GetCapacity;
    property Count: Integer read GetCount;
    property Settings: TMBASegmentSettings read fSettings;
    property ReservedBlockSize: TMemSize read fReservedBlockSize;
    property BlockCount: Integer read fBlockCount;
    property MemorySize: TMemSize read fMemorySize;
    property Memory: Pointer read fMemory;
    property AllocationMap[Index: Integer]: Boolean read GetIsAllocated;
    property Blocks[Index: Integer]: Pointer read GetBlockAddress; default;
  end;

{===============================================================================
--------------------------------------------------------------------------------
                                 TMassBlockAlloc
--------------------------------------------------------------------------------
===============================================================================}
type
  TMBAAllocatorSettings = record
    FreeEmptySegments:  Boolean;
    ThreadProtection:   Boolean;
    CacheSettings:      record
      Enable:             Boolean;
      Length:             Integer;
      TrustedReturns:     Boolean;
      AsynchronousFill:   record
        Enable:             Boolean;
        Interruptable:      Boolean;        
        CycleLength:        UInt32;
      end;
    end;
    SegmentSettings:  TMBASegmentSettings;
  end;

const
  DefaultAllocatorSettings: TMBAAllocatorSettings = (
    FreeEmptySegments:  False;
    ThreadProtection:   True;
    CacheSettings:      (
      Enable:             True;
      Length:             128;
      TrustedReturns:     True;
      AsynchronousFill:   (
        Enable:             False;
        Interruptable:      True;
        CycleLength:        1000));
    SegmentSettings:    (
      FailOnUnfreed:      True;
      MapInSegment:       False;
      BlockSize:          0;      // needs to be set by user
      Alignment:          maNone;
      SizingStyle:        szMinCount;
      BlockCount:         0));    // needs to be set by user

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
type
  TMBACache = record
    Enabled:      Boolean;
    Data:         array of Pointer; // must be thread protected
    Count:        Integer;          // -||-
    AsyncFill:    record
      Enabled:        Boolean;
      Interrupts:     Boolean;
      InterruptFlag:  Integer;      // interlocked access only
      CycleEvent:     TEvent;
      FillerThread:   TThread;
    end;
  end;
  PMBACache = ^TMBACache;

{===============================================================================
    TMassBlockAlloc - class declaration
===============================================================================}
type
  TMassBlockAlloc = class(TCustomListObject)
  protected
    fSettings:      TMBAAllocatorSettings;
    fSegments:      array of TMBASegment;
    fSegmentCount:  Integer;
    fThreadLock:    TCriticalSection;
    fCache:         TMBACache;
    // getters setters
    Function GetSegment(Index: Integer): TMBASegment; virtual;
    // inherited list methods
    Function GetCapacity: Integer; override;
    procedure SetCapacity(Value: Integer); override;
    Function GetCount: Integer; override;
    procedure SetCount(Value: Integer); override;
    // internal list management
    Function AddSegment: Integer; virtual;
    procedure DeleteSegment(Index: Integer); virtual;
    procedure ClearSegments; virtual;
    // init/final
    procedure Initialize(Settings: TMBAAllocatorSettings); virtual;
    procedure Finalize; virtual;
    // other internals
    procedure InternalAllocateBlock(out Block: Pointer; InitMemory: Boolean); virtual;
    procedure InternalFreeBlock(var Block: Pointer); virtual;
    procedure InternalAllocateBlocks(out Blocks: array of Pointer; InitMemory: Boolean); virtual;
    procedure InternalFreeBlocks(var Blocks: array of Pointer); virtual;
    Function InternalCacheFill(AsyncFill: Boolean): Integer; virtual;
  public
    constructor Create(Settings: TMBAAllocatorSettings); overload;
    constructor Create(BlockSize: TMemSize; MinBlocksPerSegment: Integer; MemoryAlignment: TMemoryAlignment = maNone); overload;
    destructor Destroy; override;
    Function LowIndex: Integer; override;
    Function HighIndex: Integer; override;
    procedure ThreadLockAcquire; virtual;
    procedure ThreadLockRelease; virtual;
    procedure AllocationAcquire; virtual;
    procedure AllocationRelease; virtual;
    // single block (de)allocation
    procedure AllocateBlock(out Block: Pointer; InitMemory: Boolean = False); overload; virtual;
    Function AllocateBlock(InitMemory: Boolean = False): Pointer; overload; virtual;
    procedure FreeBlock(var Block: Pointer); virtual;
    // multiple blocks (de)allocation
    procedure AllocateBlocks(out Blocks: array of Pointer; InitMemory: Boolean = False); virtual;
    procedure FreeBlocks(var Blocks: array of Pointer); virtual;
    // buffer (de)allocation
    procedure AllocateBuffer(out Buffer: Pointer; BufferSize: TMemSize; InitMemory: Boolean = False); virtual;
    procedure FreeBuffer(var Buffer: Pointer; BufferSize: TMemSize); virtual;
    // block vector (de)allocation
    procedure AllocateBlockVector(out Vector: Pointer; VectorLength: Integer; InitMemory: Boolean = False); virtual;
    procedure FreeBlockVector(var Vector: Pointer; VectorLength: Integer); virtual;
    // cache management
    Function CacheCount: Integer; virtual;
    procedure CacheFill(ForceSynchronous: Boolean = False); virtual;
    // cached (de)allocation
    procedure CacheAllocateBlock(out Block: Pointer; InitMemory: Boolean = False); overload; virtual;
    Function CacheAllocateBlock(InitMemory: Boolean = False): Pointer; overload; virtual;
    procedure CacheFreeBlock(var Block: Pointer); virtual;
    procedure CacheAllocateBlocks(out Blocks: array of Pointer; InitMemory: Boolean = False); virtual;
    procedure CacheFreeBlocks(var Blocks: array of Pointer); virtual;
    {$message 'todo'}
    //Function PrepareFor(Count: Integer): Integer; virtual;
    // return all blocks from an empty segment (existing or newly created)
    //procedure BurstAllocateBlocks(out Blocks: TMBAPointerArray); virtual;
    // informations and statistics (note that all following functions are subject to thread locking)
    Function SegmentSize: TMemSize; virtual;
    Function BlocksPerSegment: Integer; virtual;
    Function AllocatedBlockCount: Integer; virtual;
    Function FreeBlockCount: Integer; virtual;
    Function TotalReservedMemory: TMemSize; virtual;
    Function TotalBlocksMemory: TMemSize; virtual;
    Function TotalAllocatedMemory: TMemSize; virtual;
    Function TotalWastedMemory: TMemSize; virtual;
    Function TotalMemorySize: TMemSize; virtual;
    Function MemoryEfficiency: Double; virtual;
    Function MemoryUtilization: Double; virtual;
    // properties
    property Settings: TMBAAllocatorSettings read fSettings;
    property Count: Integer read GetCount;
    property SegmentCount: Integer read GetCount;
    property Segments[Index: Integer]: TMBASegment read GetSegment; default;
  end;

implementation

uses
  {$IFDEF Windows}Windows,{$ELSE}baseunix,{$ENDIF} 
  AuxMath;

{$IFDEF FPC_DisableWarns}
  {$DEFINE FPCDWM}
  {$DEFINE W4055:={$WARN 4055 OFF}} // Conversion between ordinals and pointers is not portable}
  {$DEFINE W5024:={$WARN 5024 OFF}} // Parameter "$1" not used
{$ENDIF}

{===============================================================================
    Internals
===============================================================================}
const
  // maximum size of segment
  MBA_MAX_SEG_SZ = TMemSize({$IFDEF AllowLargeSegments}4 * {$ENDIF}{$IFDEF CPU64bit}OneGiB{$ELSE}256 * OneMiB{$ENDIF});
  // maximum size of one block
  MBA_MAX_BLK_SZ = TMemSize(128 * OneMiB);

{===============================================================================
    Externals
===============================================================================}

{$IFDEF Linux}

Function errno_ptr: pcInt; cdecl; external name '__errno_location';

Function sysconf(name: cInt): cLong; cdecl; external;

const
  _SC_PAGESIZE = 30;  {$message 'check this value'} 

{$ENDIF}

{===============================================================================
--------------------------------------------------------------------------------
                                   TMBASegment
--------------------------------------------------------------------------------
===============================================================================}
{===============================================================================
    TMBASegment - class implementation
===============================================================================}
{-------------------------------------------------------------------------------
    TMBASegment - protected methods
-------------------------------------------------------------------------------}

Function TMBASegment.GetIsAllocated(Index: Integer): Boolean;
begin
If CheckIndex(Index) then
  Result := fAllocationMap[Index]
else
  raise EMBAIndexOutOfBounds.CreateFmt('TMBASegment.GetIsAllocated: Index (%d) out of bounds.',[Index]);
end;

//------------------------------------------------------------------------------

Function TMBASegment.GetBlockAddress(Index: Integer): Pointer;
begin
If CheckIndex(Index) then
  Result := PtrAdvance(fLowAddress,Index,fReservedBlockSize)
else
  raise EMBAIndexOutOfBounds.CreateFmt('TMBASegment.GetBlockAddress: Index (%d) out of bounds.',[Index]);
end;

//------------------------------------------------------------------------------

Function TMBASegment.GetCapacity: Integer;
begin
Result := fBlockCount;
end;

//------------------------------------------------------------------------------

{$IFDEF FPCDWM}{$PUSH}W5024{$ENDIF}
procedure TMBASegment.SetCapacity(Value: Integer);
begin
// do nothing
end;
{$IFDEF FPCDWM}{$POP}{$ENDIF}

//------------------------------------------------------------------------------

Function TMBASegment.GetCount: Integer;
begin
Result := fBlockCount;
end;

//------------------------------------------------------------------------------

{$IFDEF FPCDWM}{$PUSH}W5024{$ENDIF}
procedure TMBASegment.SetCount(Value: Integer);
begin
// do nothing
end;
{$IFDEF FPCDWM}{$POP}{$ENDIF}

//------------------------------------------------------------------------------

Function TMBASegment.CalculateMemorySize: TMemSize;
var
  PredAlign:      TMemSize;
  PredAlignBytes: TMemSize;
  PageSize:       TMemSize;
  TempMemorySize: TMemSize;
begin
{
  Calculation legend:

    P ... page size
    A ... alignment bytes (expected to be integral power of two)
    B ... reserved block size

    m ... minimal memory size
    n ... size of allocation map
    c ... minimal block count

    M ... memory size (result)
}
PredAlign := AlignmentBytes(fSettings.Alignment);
PredAlignBytes := Pred(PredAlign);
PageSize := MemoryPageSize;
case fSettings.SizingStyle of
  szMinCount:
    begin
      If fSettings.BlockCount <= 0 then
        raise EMBAInvalidValue.CreateFmt('TMBASegment.CalculateProperties: Invalid minimal block count (%d).',[fSettings.BlockCount]);
      // maximum size check
      If (fReservedBlockSize * TMemSize(fSettings.BlockCount)) > MBA_MAX_SEG_SZ then
        raise EMBAInvalidValue.CreateFmt('TMBASegment.CalculateProperties: Segment would be too large (%u).',
          [PtrInt(fReservedBlockSize * TMemSize(fSettings.BlockCount))]);
      If fSettings.MapInSegment then
      {
        Calculate memory size with page granularity. Minimum size of memory
        is requested number of blocks multiplied by reserved block size plus
        worst-case alignment padding and size of allocation map.

            M = ceil(m / P) * P

              m = pred(A) + (c * B) + n

                n = ceil(c / 8)
      }
        Result := uDivCeilPow2(PredAlignBytes + uDivCeilPow2NC(fSettings.BlockCount,8) +
          (TMemSize(fSettings.BlockCount) * fReservedBlockSize),PageSize) * PageSize
      else
      {
        Here, the minimum memory size is just number of blocks multiplied by
        reserved block size plus worst-case padding. Memory size is again
        selected with page granurality.

            M = ceil(m / P) * P

              m = pred(A) + (c * B)
      }
        Result := uDivCeilPow2(PredAlignBytes + (TMemSize(fSettings.BlockCount) * fReservedBlockSize),PageSize) * PageSize;
    end;
  szMinSize:
    begin
      If (fSettings.MemorySize <= 0) or (fSettings.MemorySize > MBA_MAX_SEG_SZ) then
        raise EMBAInvalidValue.CreateFmt('TMBASegment.CalculateMemorySize: Invalid memory size (%u).',[PtrInt(fSettings.MemorySize)]);
      // ensure the memory is large enough  
      If fSettings.MemorySize < (fReservedBlockSize + PredAlignBytes + uIfThen(fSettings.MapInSegment,TMemSize(1),0)) then
        TempMemorySize := fReservedBlockSize + PredAlignBytes + uIfThen(fSettings.MapInSegment,TMemSize(1),0)
      else
        TempMemorySize := fSettings.MemorySize;
    {
      Allocate with page granularity...

          M = ceil(m / P) * P
    }
      Result := uDivCeilPow2(TempMemorySize,PageSize) * PageSize;
    end;
  szExactSize:
    begin
    {
      The memory must be large enough to accomodate at least one block with
      worst-case padding and optionally also its map (one bit for one block,
      therefore one byte).

          M >= (pred(A) + B) + 1
    }
      If fSettings.MemorySize < (fReservedBlockSize + PredAlignBytes + uIfThen(fSettings.MapInSegment,TMemSize(1),0)) then
        raise EMBAInvalidValue.CreateFmt('TMBASegment.CalculateMemorySize: Memory size (%u) too small.',[PtrInt(fSettings.MemorySize)]);
      If fSettings.MemorySize > MBA_MAX_SEG_SZ then
        raise EMBAInvalidValue.CreateFmt('TMBASegment.CalculateMemorySize: Invalid memory size (%u).',[PtrInt(fSettings.MemorySize)]);
      Result := fSettings.MemorySize;
    end;
else
  raise EMBAInvalidValue.CreateFmt('TMBASegment.CalculateMemorySize: Unknown sizing style (%d).',[Ord(fSettings.SizingStyle)]);
end;
end;

//------------------------------------------------------------------------------

Function TMBASegment.CalculateBlockCount: Integer;
var
  TempQuotient:   TMemSize;
  TempRemainder:  TMemSize;
begin
{
  Calculation legend:

    B ... reserved block size
    M ... memory size

    p ... padding for the first block (alignment offset)
    a ... misalignment of the allocated memory

    C ... block count (result)
}
If fSettings.MapInSegment then
  begin
  {
    Subtract padding from the memory size and divide remaining bits(!) by
    size of block in bits plus one bit in allocation map.

        C = floor(8((M - p) / (8B + 1)))

    A note on the actual imlementation - it is done this way to limit a risk of
    overflow in multiplication (mainly 8 * M, as M can be a large number).
  }
    uDivMod(fMemorySize - AlignmentOffset(fMemory,fSettings.Alignment),(8 * fReservedBlockSize) + 1,TempQuotient,TempRemainder);
    Result := CvtU2I32((8 * TempQuotient) + uDivFloor(8 * TempRemainder,(8 * fReservedBlockSize) + 1));
  end
{
  Here it is simple, just subtract alignment offset from memory size and divide
  what is left by reserved block size.

      C = floor((M - p) / B)
}
else Result := CvtU2I32(uDivFloorPow2(fMemorySize - AlignmentOffset(fMemory,fSettings.Alignment),fReservedBlockSize));
end;

//------------------------------------------------------------------------------

procedure TMBASegment.Initialize(const Settings: TMBASegmentSettings);
type
  TBitVectorClass = class of TBitVectorStatic;

  Function GetAllocationMapClass: TBitVectorClass;
  begin
    If (fBlockCount and 31) <> 0 then
      Result := TBitVectorStatic
    else
      Result := TBitVectorStatic32;
  end;

var
  AlignBytes: TMemSize;
begin
fSettings := Settings;
If (fSettings.BlockSize <= 0) or (fSettings.BlockSize > MBA_MAX_BLK_SZ) then
  raise EMBAInvalidValue.CreateFmt('TMBASegment.Initialize: Invalid block size (%u).',[PtrInt(fSettings.BlockSize)]);
// reserved block size must be a multiple of alignment bytes
AlignBytes := AlignmentBytes(fSettings.Alignment);
fReservedBlockSize := uDivCeilPow2(fSettings.BlockSize,AlignBytes) * AlignBytes;
// calculate memory size and allocate memory accordingly
fMemorySize := CalculateMemorySize;
If fMemorySize <= 0 then
  raise EMBAInvalidValue.CreateFmt('TMBASegment.Initialize: Invalid memory size (%u).',[PtrInt(fMemorySize)]);
GetMem(fMemory,fMemorySize);
fLowAddress := AlignedMemory(fMemory,fSettings.Alignment);
// calculate how many blocks we can use
fBlockCount := CalculateBlockCount;
If fBlockCount <= 0 then
  raise EMBAInvalidValue.CreateFmt('TMBASegment.Initialize: Invalid block count (%d).',[fBlockCount]);
// prepare allocation map
fHighAddress := PtrAdvance(fLowAddress,PtrInt(TMemSize(fBlockCount) * fReservedBlockSize));
If fSettings.MapInSegment then
  begin
    // allocation map resides in segment's memory
    fAllocationMap := GetAllocationMapClass.Create(fHighAddress,fBlockCount);
    fAllocationMap.Fill(False);
  end
// allocation map manages its own memory buffer
else fAllocationMap := GetAllocationMapClass.Create(fBlockCount,False);
end;

//------------------------------------------------------------------------------

procedure TMBASegment.Finalize;
begin
If Assigned(fAllocationMap) then
  begin
    If not fAllocationMap.IsEmpty and fSettings.FailOnUnfreed then
      raise EMBAInvalidState.CreateFmt('TMBASegment.Finalize: Not all blocks were freed (%d).',[fAllocationMap.PopCount]);
    fAllocationMap.Free;
  end;
If Assigned(fMemory) and (fMemorySize <> 0) then
  FreeMem(fMemory,fMemorySize);
end;

//------------------------------------------------------------------------------

Function TMBASegment.FindSpaceForBuffer(BufferSize: TMemSize; out RequiredBlockCount: Integer): Integer;
var
  i,j:            Integer;
  SequenceFound:  Boolean;
begin
If BufferSize > fReservedBlockSize then
  begin
  {
    Find out how many consecutive blocks is needed (note that padding between
    blocks, if present, is used too).
  }
    RequiredBlockCount := BufferBlockCount(BufferSize);
    If RequiredBlockCount > 1 then  // this should never happen, but to be sure...
      begin
        Result := -1;
        // check if there is required number of free blocks
        If RequiredBlockCount <= FreeBlockCount then
          begin
            // find if there is a consecutive sequence of free blocks of required length
            i := fAllocationMap.FirstClean;
            repeat
              If not fAllocationMap[i] then
                begin
                  SequenceFound := True;
                  For j := Succ(i){because i-th item was already checked} to Pred(i + RequiredBlockCount) do
                    If fAllocationMap[j] then
                      begin
                        SequenceFound := False;
                        i := j; // note it will still be inc()-ed further down (so do not use j + 1)
                        Break{For j};
                      end;
                  If SequenceFound then
                    begin
                      Result := i;
                      Break{repeat-until};
                    end;
                end;
              Inc(i);
            until i > (fBlockCount - RequiredBlockCount);
          end;
      end
    else Result := fAllocationMap.FirstClean;
  end
else If BufferSize > 0 then
  begin
    RequiredBlockCount := 1;
    Result := fAllocationMap.FirstClean;
  end
else raise EMBAInvalidValue.CreateFmt('TMBASegment.FindSpaceForBuffer: Invalid buffer size (%u).',[PtrInt(BufferSize)]);
end;

{-------------------------------------------------------------------------------
    TMBASegment - public methods
-------------------------------------------------------------------------------}

class Function TMBASegment.MemoryPageSize: TMemSize;
{$IFDEF Windows}
var
  SystemInfo: TSystemInfo;
begin
{$IFDEF FPC}
SystemInfo := Default(TSystemInfo);
{$ENDIF}
GetSystemInfo(SystemInfo);
Result := TMemSize(SystemInfo.dwPageSize);
{$ELSE}
var
  Temp: cLong;
begin
Temp := sysconf(_SC_PAGESIZE);
If Temp <> -1 then
  Result := TMemSize(Temp)
else
  raise EMBASystemError.CreateFmt('TMBASegment.MemoryPageSize: Failed to obtain page size (%d).',[errno_ptr^]);
{$ENDIF}
end;

//------------------------------------------------------------------------------

constructor TMBASegment.Create(const Settings: TMBASegmentSettings);
begin
inherited Create;
Initialize(Settings);
end;

//------------------------------------------------------------------------------

destructor TMBASegment.Destroy;
begin
Finalize;
inherited;
end;

//------------------------------------------------------------------------------

Function TMBASegment.LowIndex: Integer;
begin
Result := 0;
end;

//------------------------------------------------------------------------------

Function TMBASegment.HighIndex: Integer;
begin
Result := Pred(fBlockCount);
end;

//------------------------------------------------------------------------------

Function TMBASegment.BufferBlockCount(BufferSize: TMemSize): Integer;
begin
Result := CvtU2I32(uDivCeilPow2(BufferSize,fReservedBlockSize))
end;

//------------------------------------------------------------------------------

Function TMBASegment.AddressOwned(Address: Pointer; Strict: Boolean = False): Boolean;
var
  Index:  Integer;
begin
Result := False;
{$IFDEF FPCDWM}{$PUSH}W4055{$ENDIF}
If (PtrUInt(Address) >= PtrUInt(fLowAddress)) and (PtrUInt(Address) < PtrUInt(fHighAddress)) then
  begin
    If Strict then
      begin
        // get index of block to which the address points
        Index := Integer((PtrUInt(Address) - PtrUInt(fLowAddress)) div PtrUInt(fReservedBlockSize));
        If CheckIndex(Index) then
          // check whether the address is within the block size (NOT reserved block size)
          Result := (PtrUInt(Address) - PtrUInt(PtrAdvance(fLowAddress,Index,fReservedBlockSize))) < fSettings.BlockSize;
      end
    else Result := True;
  end;
{$IFDEF FPCDWM}{$POP}{$ENDIF}
end;

//------------------------------------------------------------------------------

Function TMBASegment.AddressIndexOf(Address: Pointer; Strict: Boolean = False): Integer;
var
  Index:  Integer;
begin
Result := -1;
{$IFDEF FPCDWM}{$PUSH}W4055{$ENDIF}
If (PtrUInt(Address) >= PtrUInt(fLowAddress)) and (PtrUInt(Address) < PtrUInt(fHighAddress)) then
  begin
    Index := Integer((PtrUInt(Address) - PtrUInt(fLowAddress)) div PtrUInt(fReservedBlockSize));
    If CheckIndex(Index) then
      If not Strict or ((PtrUInt(Address) - PtrUInt(PtrAdvance(fLowAddress,Index,fReservedBlockSize))) < fSettings.BlockSize) then
        Result := Index;
  end;
{$IFDEF FPCDWM}{$POP}{$ENDIF}
end;

//------------------------------------------------------------------------------

Function TMBASegment.BlockOwned(Block: Pointer): Boolean;
var
  Index:  Integer;
begin
Result := False;
{$IFDEF FPCDWM}{$PUSH}W4055{$ENDIF}
If (PtrUInt(Block) >= PtrUInt(fLowAddress)) and (PtrUInt(Block) < PtrUInt(fHighAddress)) then
  begin
    Index := Integer((PtrUInt(Block) - PtrUInt(fLowAddress)) div PtrUInt(fReservedBlockSize));
    If CheckIndex(Index) then
      Result := Block = PtrAdvance(fLowAddress,Index,fReservedBlockSize);
  end;
{$IFDEF FPCDWM}{$POP}{$ENDIF}
end;

//------------------------------------------------------------------------------

Function TMBASegment.BlockIndexOf(Block: Pointer): Integer;
var
  Index:  Integer;
begin
Result := -1;
{$IFDEF FPCDWM}{$PUSH}W4055{$ENDIF}
If (PtrUInt(Block) >= PtrUInt(fLowAddress)) and (PtrUInt(Block) < PtrUInt(fHighAddress)) then
  begin
    Index := Integer((PtrUInt(Block) - PtrUInt(fLowAddress)) div PtrUInt(fReservedBlockSize));
    If CheckIndex(Index) and (Block = PtrAdvance(fLowAddress,Index,fReservedBlockSize)) then
      Result := Index;
  end;
{$IFDEF FPCDWM}{$POP}{$ENDIF}
end;

//------------------------------------------------------------------------------

procedure TMBASegment.AllocateBlock(out Block: Pointer; InitMemory: Boolean);
var
  Index:  Integer;
begin
If not fAllocationMap.IsFull then
  begin
    Index := fAllocationMap.FirstClean;
    If CheckIndex(Index) then
      begin
        // paranoia check
        If fAllocationMap[Index] then
          raise EMBAInvalidState.CreateFmt('TMBASegment.AllocateBlock: Block %d already allocated.',[Index]);
        fAllocationMap[Index] := True;
        Block := PtrAdvance(fLowAddress,Index,fReservedBlockSize);
        If InitMemory then
          FillChar(Block^,fReservedBlockSize{yes, better clear it all},0);
      end
    else raise EMBAOutOfResources.Create('TMBASegment.AllocateBlock: Unable to allocate block.');
  end
else raise EMBAOutOfResources.Create('TMBASegment.AllocateBlock: No free block to allocate.');
end;

//------------------------------------------------------------------------------

procedure TMBASegment.FreeBlock(var Block: Pointer);
var
  Index:  Integer;
begin
Index := BlockIndexOf(Block);
If CheckIndex(Index) then
  begin
    If not fAllocationMap[Index] then
      raise EMBAInvalidState.CreateFmt('TMBASegment.FreeBlock: Block %d not allocated.',[Index]);
    fAllocationMap[Index] := False;
    Block := nil;
  end
else raise EMBAInvalidAddress.CreateFmt('TMBASegment.FreeBlock: Invalid address (%p).',[Block]);
end;

//------------------------------------------------------------------------------

Function TMBASegment.CanAllocateBuffer(BufferSize: TMemSize): Boolean;
var
  RequiredBlockCount: Integer;
begin
Result := CheckIndex(FindSpaceForBuffer(BufferSize,RequiredBlockCount));
end;

//------------------------------------------------------------------------------

Function TMBASegment.TryAllocateBuffer(out Buffer: Pointer; BufferSize: TMemSize; InitMemory: Boolean): Boolean;
var
  RequiredBlockCount: Integer;
  Index,i:            Integer;
begin
Result := False;
If BufferSize > 0 then
  begin
    Index := FindSpaceForBuffer(BufferSize,RequiredBlockCount);
    If CheckIndex(Index) and (RequiredBlockCount > 0) then
      begin
      {
        Let's be super paranoid and check whether FindSpaceForBuffer returned
        valid data.
      }
        For i := Index to Pred(Index + RequiredBlockCount) do
          If fAllocationMap[i] then
            Exit;
        // mark all blocks as allocated
        For i := Index to Pred(Index + RequiredBlockCount) do
          fAllocationMap[i] := True;
        // get the actual allocation
        Buffer := PtrAdvance(fLowAddress,Index,fReservedBlockSize);
        If InitMemory then
          FillChar(Buffer^,TMemSize(RequiredBlockCount) * fReservedBlockSize,0);
        Result := True;
      end;
  end;
end;

//------------------------------------------------------------------------------

procedure TMBASegment.AllocateBuffer(out Buffer: Pointer; BufferSize: TMemSize; InitMemory: Boolean);
begin
If BufferSize > 0 then
  begin
    If not TryAllocateBuffer(Buffer,BufferSize,InitMemory) then
      raise EMBAOutOfResources.CreateFmt('TMBASegment.AllocateBuffer: Unable to allocate buffer of size %u.',[PtrInt(BufferSize)]);
  end
else raise EMBAInvalidValue.CreateFmt('TMBASegment.AllocateBuffer: Invalid buffer size (%u).',[PtrInt(BufferSize)]);
end;

//------------------------------------------------------------------------------

procedure TMBASegment.FreeBuffer(var Buffer: Pointer; BufferSize: TMemSize);
var
  RequiredBlockCount: Integer;
  Index,i:            Integer;
begin
If BufferSize > 0 then
  begin
    Index := BlockIndexOf(Buffer);
    If CheckIndex(Index) then
      begin
        RequiredBlockCount := BufferBlockCount(BufferSize);
        If RequiredBlockCount <= (fBlockCount - Index) then
          begin
            // check if the buffer is really allocated in full length
            For i := Index to Pred(Index + RequiredBlockCount) do
              If not fAllocationMap[i] then
                raise EMBAInvalidState.CreateFmt('TMBASegment.FreeBuffer: Block %d not allocated.',[i]);
            For i := Index to Pred(Index + RequiredBlockCount) do
              fAllocationMap[i] := False;
            Buffer := nil;
          end
        else raise EMBAInvalidValue.CreateFmt('TMBASegment.FreeBuffer: Buffer too large (%u).',[PtrInt(BufferSize)]);
      end
    else raise EMBAInvalidAddress.CreateFmt('TMBASegment.FreeBuffer: Invalid address (%p).',[Buffer]);
  end
else raise EMBAInvalidValue.CreateFmt('TMBASegment.FreeBuffer: Invalid buffer size (%u).',[PtrInt(BufferSize)]);
end;

//------------------------------------------------------------------------------

Function TMBASegment.IsFull: Boolean;
begin
Result := fAllocationMap.IsFull;
end;

//------------------------------------------------------------------------------

Function TMBASegment.IsEmpty: Boolean;
begin
Result := fAllocationMap.IsEmpty;
end;

//------------------------------------------------------------------------------

Function TMBASegment.AllocatedBlockCount: Integer;
begin
Result := fAllocationMap.PopCount;
end;

//------------------------------------------------------------------------------

Function TMBASegment.FreeBlockCount: Integer;
begin
Result := fAllocationMap.Count - fAllocationMap.PopCount;
end;

//------------------------------------------------------------------------------

Function TMBASegment.ReservedMemory: TMemSize;
begin
Result := TMemSize(fBlockCount) * fReservedBlockSize;
end;

//------------------------------------------------------------------------------

Function TMBASegment.BlocksMemory: TMemSize;
begin
Result := TMemSize(fBlockCount) * fSettings.BlockSize;
end;

//------------------------------------------------------------------------------

Function TMBASegment.AllocatedMemory: TMemSize;
begin
Result := TMemSize(AllocatedBlockCount) * fSettings.BlockSize;
end;

//------------------------------------------------------------------------------

Function TMBASegment.WastedMemory: TMemSize;
begin
Result := fMemorySize - BlocksMemory - uIfThen(fSettings.MapInSegment,fAllocationMap.MemorySize,0);
end;

//------------------------------------------------------------------------------

Function TMBASegment.MemoryEfficiency: Double;
begin
If fMemorySize <> 0 then
  Result := (fMemorySize - WastedMemory) / fMemorySize
else
  Result := 0.0;
end;

//------------------------------------------------------------------------------

Function TMBASegment.MemoryUtilization: Double;
begin
If BlocksMemory <> 0 then
  Result := AllocatedMemory / BlocksMemory
else
  Result := 0.0;
end;


{===============================================================================
--------------------------------------------------------------------------------
                               TMBAAsyncFillThread
--------------------------------------------------------------------------------
===============================================================================}
{===============================================================================
    TMBAAsyncFillThread - class declaration
===============================================================================}
type
  TMBAAsyncFillThread = class(TThread)
  protected
    fAllocator:     TMassBlockAlloc;
    fTimeout:       UInt32;
    fCycleEvent:    TEvent;
    fTerminateFlag: Integer;
    procedure Execute; override;
  public
    constructor Create(Allocator: TMassBlockAlloc);
    procedure Terminate; virtual; // hides inherited static method
  end;

{===============================================================================
    TMBAAsyncFillThread - class implementation
===============================================================================}
{-------------------------------------------------------------------------------
    TMBAAsyncFillThread - protected methods
-------------------------------------------------------------------------------}

procedure TMBAAsyncFillThread.Execute;
begin
while InterlockedExchangeAdd(fTerminateFlag,0) = 0 do
  If fCycleEvent.WaitFor(fTimeout) <> wrError then
    begin
      fAllocator.ThreadLockAcquire;
      try
        fAllocator.InternalCacheFill(True);
      finally
        fAllocator.ThreadLockRelease;
      end;
    end;
end;

{-------------------------------------------------------------------------------
    TMBAAsyncFillThread - public methods
-------------------------------------------------------------------------------}

constructor TMBAAsyncFillThread.Create(Allocator: TMassBlockAlloc);
begin
inherited Create(False);
FreeOnTerminate := False;
fAllocator := Allocator;
fTimeout := fAllocator.Settings.CacheSettings.AsynchronousFill.CycleLength;
fCycleEvent := fAllocator.fCache.AsyncFill.CycleEvent;
fTerminateFlag := 0;
end;

//------------------------------------------------------------------------------

procedure TMBAAsyncFillThread.Terminate;
begin
InterlockedIncrement(fTerminateFlag);
end;

{===============================================================================
--------------------------------------------------------------------------------
                                 TMassBlockAlloc                                                                  
--------------------------------------------------------------------------------
===============================================================================}
{===============================================================================
    TMassBlockAlloc - class implementation
===============================================================================}
{-------------------------------------------------------------------------------
    TMassBlockAlloc - protected methods
-------------------------------------------------------------------------------}

Function TMassBlockAlloc.GetSegment(Index: Integer): TMBASegment;
begin
Result := nil;
ThreadLockAcquire;
try
  If CheckIndex(Index) then
    Result := fSegments[Index]
  else
    raise EMBAIndexOutOfBounds.CreateFmt('TMassBlockAlloc.GetSegment: Index (%d) out of bounds.',[Index]);
finally
  ThreadLockRelease;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.GetCapacity: Integer;
begin
ThreadLockAcquire;
try
  Result := Length(fSegments);
finally
  ThreadLockRelease;
end;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.SetCapacity(Value: Integer);
begin
If Value >= 0 then
  begin
    ThreadLockAcquire;
    try
      If Value <> Length(fSegments) then
        begin
          If Value < fSegmentCount then
            raise EMBAInvalidAction.Create('TMassBlockAlloc.SetCapacity: Cannot lower capacity below count.');
          SetLength(fSegments,Value);
        end;
    finally
      ThreadLockRelease;
    end;
  end
else raise EMBAInvalidValue.CreateFmt('TMassBlockAlloc.SetCapacity: Invalid capacity (%d).',[Value]);
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.GetCount: Integer;
begin
ThreadLockAcquire;
try
  Result := fSegmentCount;
finally
  ThreadLockRelease;
end;
end;

//------------------------------------------------------------------------------

{$IFDEF FPCDWM}{$PUSH}W5024{$ENDIF}
procedure TMassBlockAlloc.SetCount(Value: Integer);
begin
// do nothing
end;
{$IFDEF FPCDWM}{$POP}{$ENDIF}

//------------------------------------------------------------------------------

Function TMassBlockAlloc.AddSegment: Integer;
begin
Grow;
Result := fSegmentCount;
fSegments[Result] := TMBASegment.Create(fSettings.SegmentSettings);
Inc(fSegmentCount);
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.DeleteSegment(Index: Integer);
var
  i:  Integer;
begin
If CheckIndex(Index) then
  begin
    fSegments[Index].Free;
    For i := Index to Pred(HighIndex) do
      fSegments[i] := fSegments[i + 1];
    fSegments[HighIndex] := nil;
    Dec(fSegmentCount);
    Shrink;
  end
else raise EMBAIndexOutOfBounds.CreateFmt('TMassBlockAlloc.DeleteSegment: Index (%d) out of bounds.',[Index]);
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.ClearSegments;
var
  i:  Integer;
begin
For i := LowIndex to HighIndex do
  fSegments[i].Free;
SetLength(fSegments,0);
fSegmentCount := 0;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.Initialize(Settings: TMBAAllocatorSettings);
begin
// first do settings sanity checks
If Settings.CacheSettings.Enable and (Settings.CacheSettings.Length <= 0) then
  raise EMBAInvalidValue.CreateFmt('TMassBlockAlloc.Initialize: Cache length too small (%d).',[Settings.CacheSettings.Length]);
If (Settings.SegmentSettings.BlockSize <= 0) or (Settings.SegmentSettings.BlockSize > MBA_MAX_BLK_SZ) then
  raise EMBAInvalidValue.CreateFmt('TMassBlockAlloc.Initialize: Invalid block size (%u).',[PtrInt(Settings.SegmentSettings.BlockSize)]);
case Settings.SegmentSettings.SizingStyle of
  szMinCount:
    If Settings.SegmentSettings.BlockCount <= 0 then
      raise EMBAInvalidValue.CreateFmt('TMassBlockAlloc.Initialize: Invalid block count (%d).',[Settings.SegmentSettings.BlockCount]);
  szMinSize,
  szExactSize:
    If (Settings.SegmentSettings.MemorySize <= 0) or (Settings.SegmentSettings.MemorySize > MBA_MAX_SEG_SZ) then
      raise EMBAInvalidValue.CreateFmt('TMassBlockAlloc.Initialize: Invalid memory size (%u).',[PtrInt(Settings.SegmentSettings.MemorySize)]);
else
  raise EMBAInvalidValue.CreateFmt('TMassBlockAlloc.Initialize: Unknown sizing style (%d).',[Ord(Settings.SegmentSettings.SizingStyle)]);
end;
fSettings := Settings;
fSegments := nil;
fSegmentCount := 0;
If fSettings.ThreadProtection then
  fThreadLock := SyncObjs.TCriticalSection.Create
else
  fThreadLock := nil;
// prepare cache
FillChar(fCache,SizeOf(TMBACache),0);
If fSettings.CacheSettings.Enable then
  begin
    fCache.Enabled := True;
    SetLength(fCache.Data,fSettings.CacheSettings.Length * 2);
    fCache.Count := 0;
    InternalCacheFill(False);
    If fSettings.CacheSettings.AsynchronousFill.Enable and fSettings.ThreadProtection then
      begin
        // prepare asynchronous filling
        fCache.AsyncFill.Enabled := True;
        fCache.AsyncFill.Interrupts := fSettings.CacheSettings.AsynchronousFill.Interruptable;
        fCache.AsyncFill.InterruptFlag := 0;
        fCache.AsyncFill.CycleEvent := TEvent.Create(nil,False,False,'');
        fCache.AsyncFill.FillerThread := TMBAAsyncFillThread.Create(Self);
      end;
  end;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.Finalize;
var
  i:  Integer;
begin
If fCache.AsyncFill.Enabled then
  begin
    // finalize asynch filling
    TMBAAsyncFillThread(fCache.AsyncFill.FillerThread).Terminate;
    fCache.AsyncFill.CycleEvent.SetEvent;
  {
    We need to release the thread lock here, otherwise the filler thread can
    enter a deadlock.
  }
    fCache.AsyncFill.FillerThread.WaitFor;
    FreeAndNil(fCache.AsyncFill.FillerThread);
    FreeAndNil(fCache.AsyncFill.CycleEvent);
  end;
// empty cache and free remaining segments
ThreadLockAcquire;
try
  // there is no need to check whether cache is enabled
  For i := Low(fCache.Data) to Pred(fCache.Count) do
    InternalFreeBlock(fCache.Data[i]);
  SetLength(fCache.Data,0);
  ClearSegments;
finally
  ThreadLockRelease;
end;
FreeAndNil(fThreadLock);
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.InternalAllocateBlock(out Block: Pointer; InitMemory: Boolean);
var
  i:  Integer;
begin
// first try to allocate in existing segments
For i := HighIndex downto LowIndex do
  If not fSegments[i].IsFull then
    begin
      fSegments[i].AllocateBlock(Block,InitMemory);
      Exit;
    end;
// no free block in existing segments, add new one
fSegments[AddSegment].AllocateBlock(Block,InitMemory);
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.InternalFreeBlock(var Block: Pointer);
var
  i:  Integer;
begin
For i := HighIndex downto LowIndex do
  If fSegments[i].BlockOwned(Block) then
    begin
      fSegments[i].FreeBlock(Block);
      If fSegments[i].IsEmpty and fSettings.FreeEmptySegments then
        DeleteSegment(i);
      Exit;
    end;
raise EMBAInvalidAddress.CreateFmt('TMassBlockAlloc.InternalFreeBlock: Unable to free block (%p).',[Block]);
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.InternalAllocateBlocks(out Blocks: array of Pointer; InitMemory: Boolean);
var
  Index:  Integer;
  i:      Integer;

  procedure AllocateFromSegment(SegmentIndex: Integer);
  begin
    while (Index <= High(Blocks)) and not fSegments[SegmentIndex].IsFull do
      begin
        fSegments[SegmentIndex].AllocateBlock(Blocks[Index],InitMemory);
        Inc(Index);
      end;
  end;

begin
Index := Low(Blocks);
For i := HighIndex downto LowIndex do
  begin
    AllocateFromSegment(i);
    If Index > High(Blocks) then
      Exit;
  end;
while Index <= High(Blocks) do
  AllocateFromSegment(AddSegment);
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.InternalFreeBlocks(var Blocks: array of Pointer);
var
  Index,i:  Integer;
  Counter:  Integer;
begin
Counter := Length(Blocks);
For i := HighIndex downto LowIndex do
  begin
    For Index := Low(Blocks) to High(Blocks) do
      If Assigned(Blocks[Index]) and fSegments[i].BlockOwned(Blocks[Index]) then
        begin
          fSegments[i].FreeBlock(Blocks[Index]);  // sets the pointer to nil
          Dec(Counter);
          If fSegments[i].IsEmpty then
            begin
              If fSettings.FreeEmptySegments then
                DeleteSegment(i);
              Break{For Index};
            end;
          If Counter <= 0 then
            Exit;
        end;
  end;
If Counter > 0 then
  raise EMBAInvalidAddress.CreateFmt('TMassBlockAlloc.InternalFreeBlocks: Not all blocks were freed (%d).',[Counter]);
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.InternalCacheFill(AsyncFill: Boolean): Integer;
var
  i:  Integer;
begin
Result := 0;
// thread lock must be acquired before calling this function
If fCache.Count < (Length(fCache.Data) shr 1) then
  begin
    // cache is underfilled
    For i := fCache.Count to Pred(Length(fCache.Data) div 2) do
      begin
        If AsyncFill and fCache.AsyncFill.Interrupts then
          If InterlockedExchangeAdd(fCache.AsyncFill.InterruptFlag,0) <> 0 then
            Exit; // interrupt filling
        InternalAllocateBlock(fCache.Data[i],False);
        Inc(fCache.Count);
        Inc(Result);
      end;
  end
else If fCache.Count > (Length(fCache.Data) shr 1) then
  begin
    // cache is overfilled
    For i := Pred(fCache.Count) downto (Length(fCache.Data) div 2) do
      begin
        If AsyncFill and fCache.AsyncFill.Interrupts then
          If InterlockedExchangeAdd(fCache.AsyncFill.InterruptFlag,0) <> 0 then
            Exit;
        InternalFreeBlock(fCache.Data[i]);
        Dec(fCache.Count);
        Dec(Result);
      end;
  end;
end;

{-------------------------------------------------------------------------------
    TMassBlockAlloc - public methods
-------------------------------------------------------------------------------}

constructor TMassBlockAlloc.Create(Settings: TMBAAllocatorSettings);
begin
inherited Create;
Initialize(Settings);
end;

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

constructor TMassBlockAlloc.Create(BlockSize: TMemSize; MinBlocksPerSegment: Integer; MemoryAlignment: TMemoryAlignment = maNone);
var
  Settings: TMBAAllocatorSettings;
begin
Settings := DefaultAllocatorSettings;   
Settings.SegmentSettings.BlockSize := BlockSize;
Settings.SegmentSettings.Alignment := MemoryAlignment;
Settings.SegmentSettings.SizingStyle := szMinCount;
Settings.SegmentSettings.BlockCount := MinBlocksPerSegment;
Create(Settings);
end;

//------------------------------------------------------------------------------

destructor TMassBlockAlloc.Destroy;
begin
Finalize;
inherited;
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.LowIndex: Integer;
begin
ThreadLockAcquire;
try
  Result := Low(fSegments);
finally
  ThreadLockRelease;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.HighIndex: Integer;
begin
ThreadLockAcquire;
try
  Result := Pred(fSegmentCount);
finally
  ThreadLockRelease;
end;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.ThreadLockAcquire;
begin
If Assigned(fThreadLock) then
  fThreadLock.Enter;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.ThreadLockRelease;
begin
If Assigned(fThreadLock) then
  fThreadLock.Leave;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.AllocationAcquire;
begin
If Assigned(fThreadLock) then
  begin
    If fCache.AsyncFill.Interrupts then
      InterlockedIncrement(fCache.AsyncFill.InterruptFlag);
    fThreadLock.Enter;
  end;
end;


//------------------------------------------------------------------------------

procedure TMassBlockAlloc.AllocationRelease;
begin
If Assigned(fThreadLock) then
  begin
    fThreadLock.Leave;
    If fCache.AsyncFill.Interrupts then
      InterlockedDecrement(fCache.AsyncFill.InterruptFlag);
  end;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.AllocateBlock(out Block: Pointer; InitMemory: Boolean = False);
begin
AllocationAcquire;
try
  InternalAllocateBlock(Block,InitMemory);
finally
  AllocationRelease;
end;
end;

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

Function TMassBlockAlloc.AllocateBlock(InitMemory: Boolean = False): Pointer;
begin
AllocateBlock(Result,InitMemory);
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.FreeBlock(var Block: Pointer);
begin
AllocationAcquire;
try
  InternalFreeBlock(Block);
finally
  AllocationRelease;
end;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.AllocateBlocks(out Blocks: array of Pointer; InitMemory: Boolean = False);
begin
If Length(Blocks) > 0 then
  begin
    AllocationAcquire;
    try
      InternalAllocateBlocks(Blocks,InitMemory);
    finally
      AllocationRelease;
    end;
  end;    
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.FreeBlocks(var Blocks: array of Pointer);
begin
If Length(Blocks) > 0 then
  begin
    AllocationAcquire;
    try
      InternalFreeBlocks(Blocks);
    finally
      AllocationRelease;
    end;
  end;    
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.AllocateBuffer(out Buffer: Pointer; BufferSize: TMemSize; InitMemory: Boolean = False);
var
  i:  Integer;
begin
If BufferSize > 0 then
  begin
    AllocationAcquire;
    try
      // we need at least one segment to exist for checks
      If fSegmentCount <= 0 then
        AddSegment;
      If fSegments[LowIndex].BufferBlockCount(BufferSize) > fSegments[LowIndex].BlockCount then
        raise EMBAOutOfResources.CreateFmt('TMassBlockAlloc.AllocateBuffer: Buffer is too large (%u).',[PtrInt(BufferSize)]);
      // try to alocate it somewhere
      For i := HighIndex downto LowIndex do
        If fSegments[i].TryAllocateBuffer(Buffer,BufferSize,InitMemory) then
          Exit;
      // buffer not allocated in existing segments, add a new one and allocate there
      fSegments[AddSegment].AllocateBuffer(Buffer,BufferSize,InitMemory);
    finally
      AllocationRelease;
    end;
  end
else raise EMBAInvalidValue.CreateFmt('TMassBlockAlloc.AllocateBuffer: Invalid buffer size (%u).',[PtrInt(BufferSize)]);
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.FreeBuffer(var Buffer: Pointer; BufferSize: TMemSize);
var
  i:  Integer;
begin
If BufferSize > 0 then
  begin
    AllocationAcquire;
    try
      // note that buffer size is checked by the segment (FreeBuffer)
      For i := HighIndex downto LowIndex do
        If fSegments[i].BlockOwned(Buffer) then
          begin
            fSegments[i].FreeBuffer(Buffer,BufferSize);
            If fSegments[i].IsEmpty and fSettings.FreeEmptySegments then
              DeleteSegment(i);
            Exit;
          end;
      raise EMBAInvalidAddress.CreateFmt('TMassBlockAlloc.FreeBuffer: Unable to free buffer (%p).',[Buffer]);
    finally
      AllocationRelease;
    end;
  end
else raise EMBAInvalidValue.CreateFmt('TMassBlockAlloc.FreeBuffer: Invalid buffer size (%u).',[PtrInt(BufferSize)]);
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.AllocateBlockVector(out Vector: Pointer; VectorLength: Integer; InitMemory: Boolean = False);
begin
If VectorLength > 0 then
  AllocateBuffer(Vector,TMemSize(VectorLength) * fSettings.SegmentSettings.BlockSize,InitMemory)
else
  raise EMBAInvalidValue.CreateFmt('TMassBlockAlloc.AllocateBlockVector: Invalid vector length (%d).',[VectorLength]);
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.FreeBlockVector(var Vector: Pointer; VectorLength: Integer);
begin
If VectorLength > 0 then
  FreeBuffer(Vector,TMemSize(VectorLength) * fSettings.SegmentSettings.BlockSize)
else
  raise EMBAInvalidValue.CreateFmt('TMassBlockAlloc.FreeBlockVector: Invalid vector length (%d).',[VectorLength]);
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.CacheCount: Integer;
begin
ThreadLockAcquire;
try
  Result := fCache.Count;
finally
  ThreadLockRelease;
end;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.CacheFill(ForceSynchronous: Boolean = False);
begin
If fCache.Enabled then
  begin
    If ForceSynchronous or not fCache.AsyncFill.Enabled then
      begin
        ThreadLockAcquire;
        try
          InternalCacheFill(False);
        finally
          ThreadLockRelease;
        end;
      end
    else fCache.AsyncFill.CycleEvent.SetEvent;
  end;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.CacheAllocateBlock(out Block: Pointer; InitMemory: Boolean = False);
begin
AllocationAcquire;
try
  If fCache.Count > 0 then
    begin
      Block := fCache.Data[Pred(fCache.Count)];
      fCache.Data[Pred(fCache.Count)] := nil;
      Dec(fCache.Count);
      If InitMemory then
        FillChar(Block^,fSegments[LowIndex].ReservedBlockSize,0);
    end
  else InternalAllocateBlock(Block,InitMemory);
finally
  AllocationRelease;
end;
end;

// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

Function TMassBlockAlloc.CacheAllocateBlock(InitMemory: Boolean = False): Pointer;
begin
CacheAllocateBlock(Result,InitMemory);
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.CacheFreeBlock(var Block: Pointer);

  Function CheckBlockValidity: Boolean;
  var
    i:  Integer;
  begin
    Result := False;
    For i := LowIndex to HighIndex do
      If fSegments[i].BlockOwned(Block) then
        begin
          Result := True;
          Break{For ii};
        end;
  end;

begin
AllocationAcquire;
try
  If fCache.Count < Length(fCache.Data) then
    begin
      If not fSettings.CacheSettings.TrustedReturns then
        If not CheckBlockValidity then
          raise EMBAInvalidAddress.CreateFmt('TMassBlockAlloc.CacheFreeBlock: Invalid block address (%p).',[Block]); 
      fCache.Data[fCache.Count] := Block;
      Inc(fCache.Count);
      Block := nil;
    end
  else InternalFreeBlock(Block);
finally
  AllocationRelease;
end;
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.CacheAllocateBlocks(out Blocks: array of Pointer; InitMemory: Boolean = False);
var
  Index:      Integer;
  TempBlocks: array of Pointer;
  i:          Integer;
begin
If Length(Blocks) > 0 then
  begin
    If fCache.Enabled then
      begin
        AllocationAcquire;
        try
          Index := Low(Blocks);
          while fCache.Count > 0 do
            begin
              Blocks[Index] := fCache.Data[Pred(fCache.Count)];
              fCache.Data[Pred(fCache.Count)] := nil;
              Dec(fCache.Count);
              If InitMemory then
                FillChar(Blocks[Index]^,fSegments[LowIndex].ReservedBlockSize,0);
              Inc(Index);
              If Index > High(Blocks) then
                Break{while};
            end;
          If Index < High(Blocks) then
            begin
              // more than one block needs to be allocated
              SetLength(TempBlocks,Length(Blocks) - Index);
              InternalAllocateBlocks(TempBlocks,InitMemory);
              For i := Low(TempBlocks) to High(TempBlocks) do
                Blocks[Index + i] := TempBlocks[i];
            end
          else If Index = High(Blocks) then
            // only one block needs to be allocated
            InternalAllocateBlock(Blocks[Index],InitMemory);
        finally
          AllocationRelease;
        end;
      end
    else AllocateBlocks(Blocks,InitMemory);
  end
end;

//------------------------------------------------------------------------------

procedure TMassBlockAlloc.CacheFreeBlocks(var Blocks: array of Pointer);

  Function CheckBlocksValidity(out FaultIndex: Integer): Boolean;
  var
    i,j: Integer;
  begin
    Result := False;
    FaultIndex := -1;
    For i := Low(Blocks) to High(Blocks) do
      begin
        Result := False;
        For j := LowIndex to HighIndex do
          If fSegments[j].BlockOwned(Blocks[i]) then
            begin
              Result := True;
              Break{For j};
            end;
        If not Result then
          begin
            FaultIndex := i;
            Break{For i};
          end;
      end;
  end;

var
  Index:      Integer;
  TempBlocks: array of Pointer;
  i:          Integer;
begin
If Length(Blocks) > 0 then
  begin
    If fCache.Enabled then
      begin
        AllocationAcquire;
        try
          If not fSettings.CacheSettings.TrustedReturns then
            If not CheckBlocksValidity(Index) then
              raise EMBAInvalidAddress.CreateFmt('TMassBlockAlloc.CacheFreeBlocks: Invalid block address (%p).',[Blocks[Index]]);
          Index := Low(Blocks);
          while fCache.Count < Length(fCache.Data) do
            begin
              fCache.Data[fCache.Count] := Blocks[Index];
              Blocks[Index] := nil;
              Inc(fCache.Count);
              Inc(Index);
              If Index > High(Blocks) then
                Break{while};
            end;
          If Index < High(Blocks) then
            begin
              // several blocks needs to be freed
              SetLength(TempBlocks,Length(Blocks) - Index);
              For i := Low(TempBlocks) to High(TempBlocks) do
                begin
                  TempBlocks[i] := Blocks[Index + i];
                  Blocks[Index + i] := nil;
                end;
              InternalFreeBlocks(TempBlocks);
            end
          else If Index = High(Blocks) then
            // only one block needs to be freed
            InternalFreeBlock(Blocks[Index]);
        finally
          AllocationRelease;
        end;
      end
    else FreeBlocks(Blocks);
  end
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.SegmentSize: TMemSize;
begin
ThreadLockAcquire;
try
  If fSegmentCount <= 0 then
    AddSegment;
  Result := fSegments[LowIndex].MemorySize;
finally
  ThreadLockRelease;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.BlocksPerSegment: Integer;
begin
ThreadLockAcquire;
try
  If fSegmentCount <= 0 then
    AddSegment;
  Result := fSegments[LowIndex].BlockCount;
finally
  ThreadLockRelease;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.AllocatedBlockCount: Integer;
var
  i:  Integer;
begin
ThreadLockAcquire;
try
  Result := 0;
  For i := LowIndex to HighIndex do
    Inc(Result,fSegments[i].AllocatedBlockCount);
finally
  ThreadLockRelease;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.FreeBlockCount: Integer;
var
  i:  Integer;
begin
ThreadLockAcquire;
try
  Result := 0;
  For i := LowIndex to HighIndex do
    Inc(Result,fSegments[i].FreeBlockCount);
finally
  ThreadLockRelease;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.TotalReservedMemory: TMemSize;
var
  i:  Integer;
begin
ThreadLockAcquire;
try
  Result := 0;
  For i := LowIndex to HighIndex do
    Inc(Result,fSegments[i].ReservedMemory);
finally
  ThreadLockRelease;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.TotalBlocksMemory: TMemSize;
var
  i:  Integer;
begin
ThreadLockAcquire;
try
  Result := 0;
  For i := LowIndex to HighIndex do
    Inc(Result,fSegments[i].BlocksMemory);
finally
  ThreadLockRelease;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.TotalAllocatedMemory: TMemSize;
var
  i:  Integer;
begin
ThreadLockAcquire;
try
  Result := 0;
  For i := LowIndex to HighIndex do
    Inc(Result,fSegments[i].AllocatedMemory);
finally
  ThreadLockRelease;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.TotalWastedMemory: TMemSize;
var
  i:  Integer;
begin
ThreadLockAcquire;
try
  Result := 0;
  For i := LowIndex to HighIndex do
    Inc(Result,fSegments[i].WastedMemory);
finally
  ThreadLockRelease;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.TotalMemorySize: TMemSize;
var
  i:  Integer;
begin
ThreadLockAcquire;
try
  Result := 0;
  For i := LowIndex to HighIndex do
    Inc(Result,fSegments[i].MemorySize);
finally
  ThreadLockRelease;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.MemoryEfficiency: Double;
var
  MemorySize: TMemSize;
begin
ThreadLockAcquire;
try
  MemorySize := TotalMemorySize;
  If MemorySize <> 0 then
    Result := (MemorySize - TotalWastedMemory) / MemorySize
  else
    Result := 0.0;
finally
  ThreadLockRelease;
end;
end;

//------------------------------------------------------------------------------

Function TMassBlockAlloc.MemoryUtilization: Double;
var
  BlocksMemory: TMemSize;
begin
ThreadLockAcquire;
try
  BlocksMemory := TotalBlocksMemory;
  If BlocksMemory <> 0 then
    Result := TotalAllocatedMemory / BlocksMemory
  else
    Result := 0.0;
finally
  ThreadLockRelease;
end;
end;

end.
